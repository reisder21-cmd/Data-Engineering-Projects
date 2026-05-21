import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Step 1 import the functions
from pyspark.sql.functions import (
    col,countDistinct, count, sum as spark_sum, min as spark_min, max as spark_max, when, percent_rank,
    trim, length, lit
)
from pyspark.sql.window import Window

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Step 2 Read initial gold table
fact = spark.read.parquet("s3://gbp-202605-gold/fact_order_items/")

# Create a rule to drop erroneous users that don't make sense

flag_check = fact.groupBy("user_id", "is_loyalty").agg(
  count("*").alias("row_count"),
  countDistinct("restaurant_id").alias("restaurant_count")
)

suspicious = flag_check.filter(
  (col("is_loyalty") == False) &
  ((col("row_count") > 500)|(col('restaurant_count') > 5))
).select("user_id").withColumn("drop_false_rows", lit(True))

fact_cleaned = fact.join(suspicious, "user_id", "left")
fact_cleaned = fact_cleaned.filter(
    ~(col("drop_false_rows").isNotNull() & (col("is_loyalty") == False))
)



# Step 3 Only get non-null values for user_id
fact_attributed = fact_cleaned.filter(col("user_id").isNotNull() &
                  (length(trim(col("user_id"))) > 0)
)



# Step 4 get it to one row per customer
customer_summary = fact_attributed.groupBy("user_id").agg(
  spark_max("is_loyalty").alias("is_loyalty"),
  countDistinct("order_id").alias("total_orders"),
  count("*").alias("total_line_items"),
  spark_sum("total_line_revenue").alias("total_revenue"),
  spark_min("creation_date").alias("first_order_date"),
  spark_max("creation_date").alias("last_order_date")
)
# Step 5, create a new column showing average order value
customer_summary = customer_summary.withColumn(
  "avg_order_value", col("total_revenue") / col("total_orders")
)
# Create window of revenue for all employees
revenue_window = Window.orderBy(col("total_revenue").desc())

# Add a percentile column
customer_summary = customer_summary.withColumn(
    "revenue_pct_rank",
    percent_rank().over(revenue_window)
)

# Create tiers based on percentile
customer_summary = customer_summary.withColumn(
    "clv_tier",
    when(col("revenue_pct_rank") <= 0.20, "High")
    .when(col("revenue_pct_rank") >= 0.80, "Low")
    .otherwise("Medium")
)


# Step 6 write to gold
customer_summary.write.mode("overwrite").parquet("s3://gbp-202605-gold/customer_summary/")


job.commit()