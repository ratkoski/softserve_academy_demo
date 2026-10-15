# src/transformations.py
# Pure PySpark transformations shared by the pipeline and the unit tests.
# No Lakeflow decorators, no I/O, no spark session creation. Just DataFrame in, DataFrame out.

from pyspark.sql import DataFrame, functions as F, Window

BBOX = dict(lamin=49.0, lomin=14.1, lamax=54.9, lomax=24.2)   # Poland

def clean_positions(df: DataFrame) -> DataFrame:
    """Type the raw position fields and normalise the callsign."""
    return (df
        .withColumn("callsign",        F.nullif(F.trim("callsign"), F.lit("")))
        .withColumn("position_ts",     F.to_timestamp(F.col("time_position")))
        .withColumn("last_contact_ts", F.to_timestamp(F.col("last_contact")))
        .withColumn("snapshot_ts",     F.to_timestamp("snapshot_ts")))

def quality_rules(bbox: dict = BBOX) -> dict:
    """Named quality rules; each maps a reason to the condition that VIOLATES it."""
    return {
        "missing_icao24":       F.col("icao24").isNull() | (F.col("icao24") == ""),
        "missing_position":     F.col("latitude").isNull() | F.col("longitude").isNull(),
        "outside_bbox":         ~F.col("latitude").between(bbox["lamin"], bbox["lamax"])
                                | ~F.col("longitude").between(bbox["lomin"], bbox["lomax"]),
        "implausible_altitude": F.col("baro_altitude") < -500,
        "implausible_speed":    F.col("velocity") > 400,
    }

def flag_positions(df: DataFrame, bbox: dict = BBOX) -> DataFrame:
    """Add rejection_reasons: an array of every rule the row violates (empty = valid)."""
    rules = quality_rules(bbox)
    reasons = F.array_compact(F.array(*[F.when(cond, F.lit(name)) for name, cond in rules.items()]))
    return df.withColumn("rejection_reasons", reasons)

def altitude_band(col):
    return (F.when(F.col("on_ground"), "ground")
             .when(col < 3000, "low")
             .when(col < 9000, "mid")
             .otherwise("cruise"))

def open_first_version(df: DataFrame, key: str = "icao24") -> DataFrame:
    """SCD2 initial-load fix: the first version of each key is valid from the beginning
    of time, so events observed before the dimension was first loaded still resolve."""
    w = Window.partitionBy(key).orderBy("__START_AT")
    return (df.withColumn("_v", F.row_number().over(w))
        .withColumn("valid_from", F.when(F.col("_v") == 1, F.lit("1900-01-01").cast("timestamp"))
                                   .otherwise(F.col("__START_AT")))
        .drop("_v"))