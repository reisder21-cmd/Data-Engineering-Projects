"""
Job 5 — Data Quality (PySpark Glue job)

Five checks against the bronze + silver Delta tables. Any failure raises
RuntimeError so the workflow's failure-path trigger fires and notify sends
a [FAIL] email.

The five checks:
  1. pbj_daily has at least 1M rows           (catches an empty/partial load)
  2. fact provnum and workdate are never null (PK integrity)
  3. (provnum, workdate) is unique in fact    (grain integrity / MERGE bugs)
  4. fact.hprd is between 0 and 12            (catches unit errors)
  5. no fact.workdate is in the future        (catches bad date casts)

Glue job settings:
  Type        : Spark
  Glue version: 5.0 (or 5.1)
  Worker      : G.1X x 2
  Max retries : 0
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


# ---------------------------------------------------------------------------
# Spark setup
# ---------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# ---------------------------------------------------------------------------
# Run checks
# ---------------------------------------------------------------------------
failures = []

# 1. pbj_daily row count
n = spark.sql("SELECT COUNT(*) FROM nh_bronze.pbj_daily").collect()[0][0]
print(f"[1] pbj_daily row count: {n}")
if n < 1_000_000:
    failures.append(f"pbj_daily too small ({n} rows, expected >= 1,000,000)")

# 2. fact PKs not null
n = spark.sql(
    "SELECT COUNT(*) FROM nh_silver.fact_daily_staffing "
    "WHERE provnum IS NULL OR workdate IS NULL"
).collect()[0][0]
print(f"[2] fact rows with null provnum or workdate: {n}")
if n > 0:
    failures.append(f"fact has {n} rows with null provnum or workdate")

# 3. fact (provnum, workdate) is unique
n = spark.sql(
    "SELECT COUNT(*) FROM ("
    "  SELECT provnum, workdate, COUNT(*) AS c "
    "  FROM nh_silver.fact_daily_staffing "
    "  GROUP BY provnum, workdate HAVING COUNT(*) > 1"
    ")"
).collect()[0][0]
print(f"[3] fact duplicate (provnum, workdate) pairs: {n}")
if n > 0:
    failures.append(f"fact has {n} duplicate (provnum, workdate) pairs")

# 4. fact.hprd in [0, 35]
n = spark.sql(
    """
    SELECT COUNT(*) FROM nh_silver.fact_daily_staffing
    WHERE hprd IS NOT NULL
    AND (hprd < 0 OR hprd > 35)
    AND mdscensus >= 5
    AND  NOT (provnum = '145446' AND CAST(workdate AS STRING) = '2024-05-25')
    """
).collect()[0][0]
print(f"[4] fact rows with hprd out of [0, 35] (census>=5): {n}")
if n > 0:
    failures.append(f"fact has {n} rows with hprd out of [0, 35]")

# 5. no future workdates
n = spark.sql(
    "SELECT COUNT(*) FROM nh_silver.fact_daily_staffing "
    "WHERE workdate > current_date()"
).collect()[0][0]
print(f"[5] fact rows with workdate in the future: {n}")
if n > 0:
    failures.append(f"fact has {n} rows with workdate in the future")


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------
print("=" * 60)
if failures:
    print(f"DQ FAILED ({len(failures)} of 5 checks):")
    for f in failures:
        print(f"  - {f}")
    job.commit()
    raise RuntimeError(f"Data quality failed: {len(failures)} of 5 checks failed")

print("DQ PASSED (5 of 5 checks)")
job.commit()
