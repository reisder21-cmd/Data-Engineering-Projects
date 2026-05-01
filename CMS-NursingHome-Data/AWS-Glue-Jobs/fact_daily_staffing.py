"""
Glue Job 3 — Build nh_silver.fact_daily_staffing from nh_bronze.pbj_daily.

Grain: one row per (provnum, workdate). MERGE on PK for idempotent re-runs.
Star-schema discipline: no provider attributes — join to dim_provider when needed.

Args (Glue Job Parameters):
  --JOB_NAME             (Glue-managed)
  --bronze_database      e.g. nh_bronze
  --bronze_table         e.g. pbj_daily
  --silver_bucket        e.g. nh-202604-silver
  --silver_database      e.g. nh_silver
  --silver_table         e.g. fact_daily_staffing

Required Glue job parameters (set in console):
  --datalake-formats         delta
  --enable-glue-datacatalog  true
  --conf                     spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
"""

import json
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from pyspark.context import SparkContext
from pyspark.sql import functions as F


# ---------- Glue / Spark bootstrap ----------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "bronze_database", "bronze_table",
     "silver_bucket", "silver_database", "silver_table"],
)
gc = GlueContext(SparkContext.getOrCreate())
spark = gc.spark_session
job = Job(gc)
job.init(args["JOB_NAME"], args)

print(f"[args] {json.dumps(args)}")

bronze_db = args["bronze_database"]
bronze_tbl = args["bronze_table"]
silver_db = args["silver_database"]
silver_tbl = args["silver_table"]
silver_uri = f"s3://{args['silver_bucket']}/{silver_tbl}/"

# ---------- Read bronze ----------
bronze = spark.table(f"{bronze_db}.{bronze_tbl}")
src_count = bronze.count()
print(f"[read] {src_count} rows from {bronze_db}.{bronze_tbl}")

# Role columns we'll roll up
ROLE_COLS = [
    "hrs_rndon", "hrs_rnadmin", "hrs_rn",
    "hrs_lpnadmin", "hrs_lpn",
    "hrs_cna", "hrs_natrn", "hrs_medaide",
]
EMP_COLS = [f"{c}_emp" for c in ROLE_COLS]
CTR_COLS = [f"{c}_ctr" for c in ROLE_COLS]


def safe_sum(cols):
    """Sum across columns, treating NULL as 0."""
    return sum(F.coalesce(F.col(c), F.lit(0.0)) for c in cols)


# ---------- Project + transform ----------
staging = bronze.select(
    # Primary key
    F.col("provnum"),
    F.to_date(F.col("workdate").cast("string"), "yyyyMMdd").alias("workdate"),
    F.col("workdate").alias("workdate_int"),  # keep original for traceability

    # Partition column
    F.col("cy_qtr"),

    # Census
    F.col("mdscensus"),

    # All role-level hours (passthrough)
    *[F.col(c) for c in ROLE_COLS],
    *[F.col(c) for c in EMP_COLS],
    *[F.col(c) for c in CTR_COLS],

    # Derived totals
    safe_sum(ROLE_COLS).alias("total_nurse_hours"),
    safe_sum(EMP_COLS).alias("total_employee_hours"),
    safe_sum(CTR_COLS).alias("total_contractor_hours"),

    # Audit
    F.col("load_date").alias("source_load_date"),
    F.current_timestamp().alias("_silver_updated_at"),
)

# Add ratio metrics that need the totals computed above
staging = staging.withColumn(
    "contractor_pct",
    F.when(F.col("total_nurse_hours") > 0,
           F.col("total_contractor_hours") / F.col("total_nurse_hours"))
).withColumn(
    "hprd",
    F.when(F.col("mdscensus") > 0,
           F.col("total_nurse_hours") / F.col("mdscensus"))
)

# Drop dupes on PK in case bronze ever has dupes
staging = staging.dropDuplicates(["provnum", "workdate"])
staging_count = staging.count()
print(f"[stage] projected to {len(staging.columns)} columns, {staging_count} unique (provnum, workdate) rows")

# ---------- Write: bootstrap on first run, MERGE thereafter ----------
spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_db}")

if not DeltaTable.isDeltaTable(spark, silver_uri):
    print(f"[write] bootstrap: no Delta table at {silver_uri}, writing initial load")
    (staging.write.format("delta")
        .mode("overwrite")
        .partitionBy("cy_qtr")
        .save(silver_uri))
    spark.sql(f"DROP TABLE IF EXISTS {silver_db}.{silver_tbl}")
    spark.sql(f"CREATE TABLE {silver_db}.{silver_tbl} USING DELTA LOCATION '{silver_uri}'")
    print(f"[catalog] registered {silver_db}.{silver_tbl}")
else:
    print(f"[write] merging into existing Delta table at {silver_uri}")
    target = DeltaTable.forPath(spark, silver_uri)
    (target.alias("tgt")
           .merge(staging.alias("src"),
                  "tgt.provnum = src.provnum AND tgt.workdate = src.workdate")
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute())
    spark.sql(f"REFRESH TABLE {silver_db}.{silver_tbl}")
    print(f"[catalog] refreshed {silver_db}.{silver_tbl}")

# ---------- Sanity check ----------
final = spark.table(f"{silver_db}.{silver_tbl}")
final_count = final.count()
distinct_dates = final.select("workdate").distinct().count()
distinct_facilities = final.select("provnum").distinct().count()
print(f"[verify] {silver_db}.{silver_tbl}: {final_count} rows, "
      f"{distinct_facilities} facilities × {distinct_dates} distinct workdates")

job.commit()
print("[done] job committed")
