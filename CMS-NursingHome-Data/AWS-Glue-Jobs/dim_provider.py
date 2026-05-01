"""
Glue Job 2 — Build nh_silver.dim_provider from nh_bronze.provider_info.

SCD Type 1: latest snapshot wins on match, insert on miss. No soft-delete.

Args (Glue Job Parameters):
  --JOB_NAME             (Glue-managed)
  --bronze_database      e.g. nh_bronze
  --bronze_table         e.g. provider_info
  --silver_bucket        e.g. nh-202604-silver
  --silver_database      e.g. nh_silver
  --silver_table         e.g. dim_provider

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


def yn_to_bool(col):
    """Map CMS 'Y'/'N' columns to boolean, NULL otherwise."""
    return F.when(F.col(col) == "Y", F.lit(True)) \
            .when(F.col(col) == "N", F.lit(False))


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

# ---------- Read latest snapshot from bronze ----------
bronze = spark.table(f"{bronze_db}.{bronze_tbl}")
latest_load = bronze.agg(F.max("load_date")).collect()[0][0]
print(f"[read] latest bronze snapshot: load_date={latest_load}")

src = bronze.where(F.col("load_date") == latest_load)
print(f"[read] {src.count()} rows from {bronze_db}.{bronze_tbl}")

# ---------- Project + clean to dim_provider schema ----------
staging = src.select(
    # Identity
    F.col("cms_certification_number_ccn").alias("ccn"),
    F.col("provider_name"),
    F.col("legal_business_name"),

    # Location (cast int → padded string for ZIP & phone)
    F.col("provider_address"),
    F.col("city_town").alias("city"),
    F.col("state"),
    F.lpad(F.col("zip_code").cast("string"), 5, "0").alias("zip_code"),
    F.col("county_parish").alias("county"),
    F.lpad(F.col("telephone_number").cast("string"), 10, "0").alias("telephone_number"),
    F.col("latitude"),
    F.col("longitude"),

    # Org / type
    F.col("ownership_type"),
    F.col("provider_type"),
    yn_to_bool("provider_resides_in_hospital").alias("provider_resides_in_hospital"),
    yn_to_bool("continuing_care_retirement_community").alias("is_ccrc"),

    # Capacity
    F.col("number_of_certified_beds").alias("certified_beds"),
    F.col("average_number_of_residents_per_day").alias("avg_residents_per_day"),

    # Compliance flags
    F.col("special_focus_status"),
    yn_to_bool("automatic_sprinkler_systems_in_all_required_areas").alias("has_sprinklers"),
    F.col("abuse_icon"),

    # Star ratings (1–5)
    F.col("overall_rating"),
    F.col("health_inspection_rating"),
    F.col("qm_rating"),
    F.col("staffing_rating"),

    # Key dates
    F.col("date_first_approved_to_provide_medicare_and_medicaid_services").alias("date_first_approved"),
    F.col("processing_date"),

    # Audit
    F.lit(latest_load).alias("snapshot_date"),
    F.current_timestamp().alias("_silver_updated_at"),
)

# Drop dupes on CCN just in case bronze has any (it shouldn't — we verified earlier)
staging = staging.dropDuplicates(["ccn"])
staging_count = staging.count()
print(f"[stage] projected to {len(staging.columns)} columns, {staging_count} unique CCNs")

# ---------- Write: bootstrap on first run, MERGE thereafter ----------
spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_db}")

if not DeltaTable.isDeltaTable(spark, silver_uri):
    # First run — write full snapshot, then register table
    print(f"[write] bootstrap: no Delta table at {silver_uri}, writing full snapshot")
    (staging.write.format("delta").mode("overwrite").save(silver_uri))
    spark.sql(f"DROP TABLE IF EXISTS {silver_db}.{silver_tbl}")
    spark.sql(f"CREATE TABLE {silver_db}.{silver_tbl} USING DELTA LOCATION '{silver_uri}'")
    print(f"[catalog] registered {silver_db}.{silver_tbl}")
else:
    # SCD1 MERGE — update on match, insert on miss
    print(f"[write] merging into existing Delta table at {silver_uri}")
    target = DeltaTable.forPath(spark, silver_uri)
    (target.alias("tgt")
           .merge(staging.alias("src"), "tgt.ccn = src.ccn")
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute())
    # Catalog already exists; refresh metadata in case schema evolved
    spark.sql(f"REFRESH TABLE {silver_db}.{silver_tbl}")
    print(f"[catalog] refreshed {silver_db}.{silver_tbl}")

# ---------- Sanity check ----------
final_count = spark.table(f"{silver_db}.{silver_tbl}").count()
print(f"[verify] {silver_db}.{silver_tbl} now has {final_count} rows")

job.commit()
print("[done] job committed")
