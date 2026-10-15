# Databricks notebook source
# src/governance/apply_policies.py
# Row-level and column-level security on the gold layer, applied as a job task
# after every gold build so the policies survive table rebuilds.
#
# RLS is table-driven: operator_access maps a user to the operators they may see
# ('*' = all). CLS masks aircraft identifiers for users not in privileged_users.
# Access lists live in tables, not in code, so no emails are committed.

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "opensky")
catalog = dbutils.widgets.get("catalog")
schema  = dbutils.widgets.get("schema")
T = lambda n: f"{catalog}.{schema}.{n}"

me = spark.sql("SELECT current_user()").collect()[0][0]

# Access tables (created once, the current user is bootstrapped with full access)
spark.sql(f"""CREATE TABLE IF NOT EXISTS {T('operator_access')} (
    user_email STRING NOT NULL, operator STRING NOT NULL) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {T('privileged_users')} (
    user_email STRING NOT NULL) USING DELTA""")

spark.sql(f"""MERGE INTO {T('operator_access')} t
    USING (SELECT '{me}' AS user_email, '*' AS operator) s
    ON t.user_email = s.user_email AND t.operator = s.operator
    WHEN NOT MATCHED THEN INSERT *""")
spark.sql(f"""MERGE INTO {T('privileged_users')} t
    USING (SELECT '{me}' AS user_email) s
    ON t.user_email = s.user_email
    WHEN NOT MATCHED THEN INSERT *""")

# Policy functions
spark.sql(f"""CREATE OR REPLACE FUNCTION {T('operator_filter')}(op STRING)
    RETURN EXISTS (
        SELECT 1 FROM {T('operator_access')} a
        WHERE a.user_email = current_user() AND (a.operator = '*' OR a.operator = op))""")

spark.sql(f"""CREATE OR REPLACE FUNCTION {T('mask_identifier')}(v STRING)
    RETURN CASE
        WHEN EXISTS (SELECT 1 FROM {T('privileged_users')} WHERE user_email = current_user()) THEN v
        WHEN v IS NULL THEN NULL
        ELSE CONCAT(LEFT(v, 2), '****') END""")

# Apply to gold serving tables
for tbl in ["current_positions", "fact_positions"]:
    spark.sql(f"ALTER TABLE {T(tbl)} SET ROW FILTER {T('operator_filter')} ON (operator)"
              if tbl == "current_positions" else
              f"ALTER TABLE {T(tbl)} DROP ROW FILTER")
    spark.sql(f"ALTER TABLE {T(tbl)} ALTER COLUMN icao24 SET MASK {T('mask_identifier')}")

spark.sql(f"ALTER TABLE {T('dim_aircraft')} SET ROW FILTER {T('operator_filter')} ON (operator)")
spark.sql(f"ALTER TABLE {T('dim_aircraft')} ALTER COLUMN registration SET MASK {T('mask_identifier')}")

print(f"Policies applied in {catalog}.{schema} for {me}")