# Databricks notebook source
# src/gold/build_gold.py
# Gold layer: star schema over silver positions, plus serving aggregates.
# Runs as a Lakeflow Job task after the pipeline. Dimensions and aggregates are
# rebuilt in full (they are small); the fact table is rebuilt in full for now —
# incremental loading via Change Data Feed is the planned CDC extension.

from pyspark.sql import functions as F, Window
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "opensky")
catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")
dbutils.widgets.text("src_root", "")

import sys, os
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
src_root = "/Workspace" + os.path.dirname(os.path.dirname(nb_path))
sys.path.append(src_root)
print(f"src_root: {src_root}")

from transformations import open_first_version, altitude_band

T = lambda name: f"{catalog}.{schema}.{name}"
batch_ts = datetime.now()

def save(df, name):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(T(name))
    print(f"{name:22s} {spark.read.table(T(name)).count():>10,}")


# DIMENSIONS

# dim_date: generated calendar, one row per day
dim_date = (spark.sql("SELECT explode(sequence(DATE'2026-01-01', DATE'2026-12-31')) AS full_date")
    .withColumn("date_key",     F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year",         F.year("full_date"))
    .withColumn("month",        F.month("full_date"))
    .withColumn("month_name",   F.date_format("full_date", "MMMM"))
    .withColumn("day_of_week",  F.when(F.dayofweek("full_date") == 1, 7).otherwise(F.dayofweek("full_date") - 1))
    .withColumn("day_name",     F.date_format("full_date", "EEEE"))
    .withColumn("is_weekend",   F.col("day_of_week") >= 6))
save(dim_date, "dim_date")

# dim_time: one row per hour - air traffic has a strong daily rhythm
dim_time = (spark.range(24).withColumnRenamed("id", "hour_key")
    .withColumn("hour_key", F.col("hour_key").cast("int"))
    .withColumn("hour_label", F.format_string("%02d:00", F.col("hour_key")))
    .withColumn("part_of_day",
        F.when(F.col("hour_key") < 6, "night")
         .when(F.col("hour_key") < 12, "morning")
         .when(F.col("hour_key") < 18, "afternoon")
         .otherwise("evening")))
save(dim_time, "dim_time")

# The first known version of an aircraft is valid from the beginning of time,
# not from the moment we first loaded the registry - otherwise positions observed
# before the initial load would never resolve to any version.
first_version = Window.partitionBy("icao24").orderBy("__START_AT")

# dim_aircraft: every SCD2 version from silver, with a surrogate key per VERSION.
# Hashing icao24 together with __START_AT is what keeps versions distinct —
# hashing the business key alone would give every version the same key.
dim_aircraft = (open_first_version(spark.read.table(T("silver_aircraft")))
    .withColumn("aircraft_key", F.xxhash64("icao24", "__START_AT"))
    .withColumn("is_current", F.col("__END_AT").isNull())
    .withColumnRenamed("__END_AT", "valid_to")
    .withColumn("operator", F.coalesce("operator", F.lit("Unknown")))
    .withColumn("manufacturername", F.coalesce("manufacturername", F.lit("Unknown")))
    .select("aircraft_key", "icao24", "registration", "manufacturername", "model",
            "typecode", "icaoaircrafttype", "operator", "operatoricao", "owner",
            "built", "categoryDescription", "valid_from", "valid_to", "is_current"))

# Sentinel row so positions of aircraft missing from the registry never drop out of a join
unknown = spark.createDataFrame(
    [(-1, "unknown", None, "Unknown", "Unknown", None, None, "Unknown", None, None, None, None,
      datetime(1900, 1, 1), None, True)],
    dim_aircraft.schema)
save(dim_aircraft.unionByName(unknown), "dim_aircraft")

# dim_airport: straight from silver, id as key
dim_airport = (spark.read.table(T("silver_airports"))
    .withColumnRenamed("id", "airport_key")
    .select("airport_key", "ident", "icao_code", "iata_code", "name", "type",
            "latitude", "longitude", "elevation_ft", "iso_country", "municipality", "wikipedia_link"))
save(dim_airport, "dim_airport")

# FACT TABLE
spark.sql(f"CREATE TABLE IF NOT EXISTS {T('cdc_watermark')} (table_name STRING, last_version BIGINT) USING DELTA")

silver_version = spark.sql(f"DESCRIBE HISTORY {T('silver_positions')} LIMIT 1").collect()[0]["version"]
wm = spark.sql(f"SELECT last_version FROM {T('cdc_watermark')} WHERE table_name = 'silver_positions'").collect()
start_version = wm[0]["last_version"] + 1 if wm else 0

fact_exists = spark.catalog.tableExists(T("fact_positions"))

if fact_exists and wm and start_version <= silver_version:
    # Incremental path: only inserts since the last processed version
    changes = (spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", start_version)
        .option("endingVersion", silver_version)
        .table(T("silver_positions"))
        .filter("_change_type = 'insert'")
        .drop("_change_type", "_commit_version", "_commit_timestamp"))
    mode = "incremental"
elif fact_exists and wm:
    changes = None   # nothing new
    mode = "noop"
else:
    changes = spark.read.table(T("silver_positions"))   # first load: full snapshot
    mode = "initial"

aircraft = spark.read.table(T("dim_aircraft")).filter("aircraft_key <> -1")

def to_fact(positions):
    return (positions.alias("p")
        .join(aircraft.alias("a"),
              (F.col("p.icao24") == F.col("a.icao24")) &
              (F.col("p.snapshot_ts") >= F.col("a.valid_from")) &
              (F.col("p.snapshot_ts") < F.coalesce(F.col("a.valid_to"), F.lit("9999-12-31").cast("timestamp"))),
              "left")
        .select(
            F.col("p.icao24"), F.col("p.callsign"), F.col("p.origin_country"),
            F.coalesce(F.col("a.aircraft_key"), F.lit(-1)).alias("aircraft_key"),
            F.date_format("p.snapshot_ts", "yyyyMMdd").cast("int").alias("date_key"),
            F.hour("p.snapshot_ts").alias("hour_key"),
            F.col("p.snapshot_ts"), F.col("p.position_ts"),
            F.col("p.latitude"), F.col("p.longitude"),
            F.col("p.baro_altitude"), F.col("p.geo_altitude"),
            F.col("p.velocity"), F.col("p.true_track"), F.col("p.vertical_rate"),
            F.col("p.on_ground"), F.col("p.squawk"))
        .withColumn("gold_loaded_at", F.lit(batch_ts).cast("timestamp")))

if mode == "initial":
    to_fact(changes).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("fact_positions"))
elif mode == "incremental":
    # MERGE on the natural key (aircraft + snapshot time) keeps re-runs idempotent:
    # a change already loaded matches and is left alone, a new one is inserted.
    (DeltaTable.forName(spark, T("fact_positions")).alias("t")
        .merge(to_fact(changes).alias("s"), "t.icao24 = s.icao24 AND t.snapshot_ts = s.snapshot_ts")
        .whenNotMatchedInsertAll()
        .execute())

spark.sql(f"""MERGE INTO {T('cdc_watermark')} t USING (SELECT 'silver_positions' AS table_name, {silver_version} AS last_version) s
             ON t.table_name = s.table_name WHEN MATCHED THEN UPDATE SET last_version = s.last_version WHEN NOT MATCHED THEN INSERT *""")

n = spark.read.table(T("fact_positions")).count()
rng = f"{start_version}→{silver_version}" if mode != "noop" else f"at version {silver_version}, nothing new"
print(f"fact_positions: {n:,} rows  (mode={mode}, silver {rng})")


# SERVING TABLES
fact = spark.read.table(T("fact_positions"))
dim_a = spark.read.table(T("dim_aircraft"))

# current_positions: latest observation per aircraft this feeds the live map
w = Window.partitionBy("icao24").orderBy(F.col("snapshot_ts").desc())
current = (fact.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
    .join(dim_a.select("aircraft_key", "registration", "manufacturername", "model", "operator", "typecode"),
          "aircraft_key", "left")
    .withColumn("altitude_band", altitude_band(F.col("baro_altitude"))))
save(current, "current_positions")

# agg_hourly_traffic: how busy the airspace is through the day
hourly = (fact.join(spark.read.table(T("dim_date")), "date_key")
    .groupBy("full_date", "day_name", "is_weekend", "hour_key")
    .agg(F.countDistinct("icao24").alias("distinct_aircraft"),
         F.count("*").alias("observations"),
         F.avg("baro_altitude").alias("avg_altitude_m"),
         F.avg("velocity").alias("avg_velocity_ms"),
         F.sum(F.col("on_ground").cast("int")).alias("on_ground_count"))
    .withColumn("gold_loaded_at", F.lit(batch_ts).cast("timestamp")))
save(hourly, "agg_hourly_traffic")

# agg_by_operator: who flies over Poland
by_operator = (fact.join(dim_a.select("aircraft_key", "operator", "manufacturername"), "aircraft_key", "left")
    .groupBy("operator", "manufacturername")
    .agg(F.countDistinct("icao24").alias("distinct_aircraft"),
         F.count("*").alias("observations"),
         F.avg("baro_altitude").alias("avg_altitude_m"))
    .withColumn("gold_loaded_at", F.lit(batch_ts).cast("timestamp")))
save(by_operator, "agg_by_operator")

# agg_by_country: registration country of observed aircraft
by_country = (fact.groupBy("origin_country")
    .agg(F.countDistinct("icao24").alias("distinct_aircraft"),
         F.count("*").alias("observations"))
    .withColumn("gold_loaded_at", F.lit(batch_ts).cast("timestamp")))
save(by_country, "agg_by_country")

# agg_kpi: single row for dashboard counters and the volume alert
kpi = (fact.agg(
        F.countDistinct("icao24").alias("distinct_aircraft"),
        F.count("*").alias("total_observations"),
        F.countDistinct("snapshot_ts").alias("snapshots"),
        F.max("snapshot_ts").alias("last_snapshot_at"),
        F.avg("baro_altitude").alias("avg_altitude_m"))
    .withColumn("gold_loaded_at", F.lit(batch_ts).cast("timestamp")))
save(kpi, "agg_kpi")

print("\nGold build complete.")