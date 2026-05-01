"""
Job 6 — Build Gold Layer (PySpark Glue job)

Reads the silver star schema (fact_daily_staffing + dim_provider) and writes
four pre-aggregated gold tables that the Streamlit dashboard reads directly:

  1. gold.facility_summary       (one row per facility,    ~14K rows)
  2. gold.state_summary          (one row per state,       ~52 rows)
  3. gold.facility_monthly_trend (provnum x year_month,    ~80K rows)
  4. gold.state_monthly_trend    (state x year_month,      ~150 rows)

All four tables are full overwrites each run — gold is derived/disposable,
the inputs are small, and full rebuild keeps the code simple.

CMS minimum HPRD = 3.48 (2024 staffing rule baseline).

Glue job settings:
  Type        : Spark
  Glue version: 5.0 or 5.1
  Worker      : G.1X x 2
  Job params  :
    --datalake-formats delta
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
    --enable-glue-datacatalog true
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

SILVER_DB = "nh_silver"
GOLD_DB = "nh_gold"
GOLD_BUCKET = "nh-202604-gold"
CMS_MIN_HPRD = 3.48

# Make sure the gold database exists.
spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")


def write_and_register(df, table_name):
    """Overwrite a Delta table at s3://{GOLD_BUCKET}/{table_name}/ and
    register it in the Glue Catalog using Spark SQL (engine v3 reads natively)."""
    uri = f"s3://{GOLD_BUCKET}/{table_name}/"
    print(f"[gold] writing {GOLD_DB}.{table_name} -> {uri}")
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(uri))
    spark.sql(f"DROP TABLE IF EXISTS {GOLD_DB}.{table_name}")
    spark.sql(f"CREATE TABLE {GOLD_DB}.{table_name} USING DELTA LOCATION '{uri}'")
    n = spark.table(f"{GOLD_DB}.{table_name}").count()
    print(f"[gold] {GOLD_DB}.{table_name}: {n} rows")


# ---------------------------------------------------------------------------
# Read silver
# ---------------------------------------------------------------------------
fact = spark.table(f"{SILVER_DB}.fact_daily_staffing")
dim = spark.table(f"{SILVER_DB}.dim_provider")

print(f"[gold] silver fact rows: {fact.count()}")
print(f"[gold] silver dim rows:  {dim.count()}")


# ---------------------------------------------------------------------------
# Table 1: gold.facility_summary
#   Per-facility aggregates joined with dim attributes.
#   Occupancy and nurse-to-patient ratio are derived from the averages.
# ---------------------------------------------------------------------------
fact_agg = (
    fact.groupBy("provnum")
        .agg(
            F.avg("hprd").alias("avg_hprd"),
            F.avg("contractor_pct").alias("avg_contractor_pct"),
            F.avg("mdscensus").alias("avg_mdscensus"),
            F.avg("total_nurse_hours").alias("avg_total_nurse_hours"),
            F.countDistinct("workdate").alias("days_reported"),
            F.min("workdate").alias("first_workdate"),
            F.max("workdate").alias("last_workdate"),
            F.sum(F.when(F.col("hprd") < CMS_MIN_HPRD, 1).otherwise(0))
                .alias("days_below_cms_minimum"),
        )
        .withColumn(
            "pct_days_below_cms_minimum",
            F.col("days_below_cms_minimum") / F.col("days_reported"),
        )
)

facility_summary = (
    fact_agg.alias("f")
        .join(dim.alias("d"), F.col("f.provnum") == F.col("d.ccn"), "inner")
        .select(
            F.col("d.ccn"),
            F.col("d.provider_name"),
            F.col("d.state"),
            F.col("d.city"),
            F.col("d.county"),
            F.col("d.ownership_type"),
            F.col("d.certified_beds"),
            F.col("d.overall_rating"),
            F.col("d.staffing_rating"),
            F.col("d.health_inspection_rating"),
            F.col("d.qm_rating"),
            F.col("f.avg_hprd"),
            F.col("f.avg_contractor_pct"),
            F.col("f.avg_mdscensus"),
            # avg_occupancy_rate = avg_mdscensus / certified_beds  (guard div-by-zero)
            F.when(F.col("d.certified_beds") > 0,
                   F.col("f.avg_mdscensus") / F.col("d.certified_beds"))
             .otherwise(F.lit(None))
             .alias("avg_occupancy_rate"),
            # avg_nurse_to_patient_ratio = residents per concurrent nurse
            #   = avg_mdscensus / (avg_total_nurse_hours / 24)
            F.when(F.col("f.avg_total_nurse_hours") > 0,
                   F.col("f.avg_mdscensus") / (F.col("f.avg_total_nurse_hours") / F.lit(24.0)))
             .otherwise(F.lit(None))
             .alias("avg_nurse_to_patient_ratio"),
            F.col("f.avg_total_nurse_hours"),
            F.col("f.days_reported"),
            F.col("f.first_workdate"),
            F.col("f.last_workdate"),
            F.col("f.days_below_cms_minimum"),
            F.col("f.pct_days_below_cms_minimum"),
        )
)

write_and_register(facility_summary, "facility_summary")


# ---------------------------------------------------------------------------
# Table 2: gold.state_summary
#   State-level rollup of facility_summary.
#   weighted_avg_* uses avg_mdscensus as the weight (mirrors CMS convention).
# ---------------------------------------------------------------------------
state_summary = (
    facility_summary.groupBy("state")
        .agg(
            F.count("*").alias("facility_count"),
            (F.sum(F.col("avg_hprd") * F.col("avg_mdscensus"))
                / F.sum("avg_mdscensus")).alias("weighted_avg_hprd"),
            F.expr("percentile_approx(avg_hprd, 0.5)").alias("median_hprd"),
            (F.sum(F.col("avg_occupancy_rate") * F.col("avg_mdscensus"))
                / F.sum("avg_mdscensus")).alias("weighted_avg_occupancy_rate"),
            F.expr("percentile_approx(avg_occupancy_rate, 0.5)")
                .alias("median_occupancy_rate"),
            F.avg(F.when(F.col("avg_hprd") < CMS_MIN_HPRD, 1.0).otherwise(0.0))
                .alias("pct_facilities_below_cms_minimum"),
            F.expr("percentile_approx(avg_contractor_pct, 0.5)")
                .alias("median_contractor_pct"),
            F.avg("overall_rating").alias("avg_overall_rating"),
        )
)

write_and_register(state_summary, "state_summary")


# ---------------------------------------------------------------------------
# Table 3: gold.facility_monthly_trend
#   Per-facility monthly aggregates. Powers facility-lookup trend lines.
# ---------------------------------------------------------------------------
monthly_fact = (
    fact.withColumn("year_month", F.date_format("workdate", "yyyy-MM"))
        .groupBy("provnum", "year_month")
        .agg(
            F.avg("hprd").alias("avg_hprd"),
            F.avg("contractor_pct").alias("avg_contractor_pct"),
            F.avg("mdscensus").alias("avg_mdscensus"),
            F.avg("total_nurse_hours").alias("avg_total_nurse_hours"),
            F.countDistinct("workdate").alias("days_reported"),
        )
)

facility_monthly_trend = (
    monthly_fact.alias("m")
        .join(dim.select("ccn", "certified_beds", "state").alias("d"),
              F.col("m.provnum") == F.col("d.ccn"), "left")
        .select(
            F.col("m.provnum"),
            F.col("d.state"),
            F.col("m.year_month"),
            F.col("m.avg_hprd"),
            F.col("m.avg_contractor_pct"),
            F.col("m.avg_mdscensus"),
            F.when(F.col("d.certified_beds") > 0,
                   F.col("m.avg_mdscensus") / F.col("d.certified_beds"))
             .otherwise(F.lit(None))
             .alias("avg_occupancy_rate"),
            F.when(F.col("m.avg_total_nurse_hours") > 0,
                   F.col("m.avg_mdscensus") / (F.col("m.avg_total_nurse_hours") / F.lit(24.0)))
             .otherwise(F.lit(None))
             .alias("avg_nurse_to_patient_ratio"),
            F.col("m.days_reported"),
        )
)

write_and_register(facility_monthly_trend, "facility_monthly_trend")


# ---------------------------------------------------------------------------
# Table 4: gold.state_monthly_trend
#   State x month rollup. Powers heatmap visuals.
# ---------------------------------------------------------------------------
state_monthly_trend = (
    facility_monthly_trend
        .where(F.col("state").isNotNull())
        .groupBy("state", "year_month")
        .agg(
            (F.sum(F.col("avg_hprd") * F.col("avg_mdscensus"))
                / F.sum("avg_mdscensus")).alias("weighted_avg_hprd"),
            (F.sum(F.col("avg_occupancy_rate") * F.col("avg_mdscensus"))
                / F.sum("avg_mdscensus")).alias("weighted_avg_occupancy_rate"),
            F.countDistinct("provnum").alias("facility_count"),
        )
)

write_and_register(state_monthly_trend, "state_monthly_trend")


# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
print("[gold] all 4 gold tables built successfully")
job.commit()
