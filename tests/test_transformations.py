import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pyspark.sql import functions as F
from chispa import assert_df_equality
from transformations import clean_positions, flag_positions, altitude_band, open_first_version

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType

POSITIONS_SCHEMA = StructType([
    StructField("icao24", StringType()),        StructField("callsign", StringType()),
    StructField("latitude", DoubleType()),      StructField("longitude", DoubleType()),
    StructField("baro_altitude", DoubleType()), StructField("velocity", DoubleType()),
    StructField("on_ground", BooleanType()),    StructField("time_position", LongType()),
    StructField("last_contact", LongType()),    StructField("snapshot_ts", StringType()),
])

def _positions(spark, rows):
    return spark.createDataFrame(rows, POSITIONS_SCHEMA)

GOOD = ("4acb03", "NSZ2097 ", 50.755, 16.98, 11277.6, 265.2, False, 1791670302, 1791670302, "2026-10-11T16:00:00+00:00")

def test_valid_position_has_no_reasons(spark):
    df = flag_positions(clean_positions(_positions(spark, [GOOD])))
    assert df.collect()[0]["rejection_reasons"] == []

def test_callsign_is_trimmed_and_empty_becomes_null(spark):
    df = clean_positions(_positions(spark, [GOOD, GOOD[:1] + ("   ",) + GOOD[2:]]))
    assert [r["callsign"] for r in df.collect()] == ["NSZ2097", None]

def test_implausible_speed_is_flagged(spark):
    row = GOOD[:5] + (1980.86,) + GOOD[6:]          # the real ADS-B glitch from the data
    df = flag_positions(clean_positions(_positions(spark, [row])))
    assert df.collect()[0]["rejection_reasons"] == ["implausible_speed"]

def test_outside_bbox_is_flagged(spark):
    row = GOOD[:2] + (60.0, 16.98) + GOOD[4:]         # Scandinavia, not Poland
    df = flag_positions(clean_positions(_positions(spark, [row])))
    assert "outside_bbox" in df.collect()[0]["rejection_reasons"]

def test_multiple_violations_are_all_recorded(spark):
    row = ("", None, None, None, -9999.0, 999.0, False, None, None, "2026-10-11T16:00:00+00:00")
    reasons = flag_positions(clean_positions(_positions(spark, [row]))).collect()[0]["rejection_reasons"]
    assert set(reasons) == {"missing_icao24", "missing_position", "implausible_altitude", "implausible_speed"}

def test_altitude_band(spark):
    df = spark.createDataFrame([(True, 0.0), (False, 1000.0), (False, 5000.0), (False, 11000.0)],
                               ["on_ground", "baro_altitude"])
    bands = [r[0] for r in df.select(altitude_band(F.col("baro_altitude"))).collect()]
    assert bands == ["ground", "low", "mid", "cruise"]

def test_first_scd2_version_is_valid_from_beginning_of_time(spark):
    df = spark.createDataFrame(
        [("abc", "2026-10-12 12:00:00"), ("abc", "2026-10-13 12:00:00")], ["icao24", "__START_AT"]
    ).withColumn("__START_AT", F.col("__START_AT").cast("timestamp"))
    out = open_first_version(df).orderBy("__START_AT").collect()
    assert str(out[0]["valid_from"]) == "1900-01-01 00:00:00"
    assert out[1]["valid_from"] == out[1]["__START_AT"]