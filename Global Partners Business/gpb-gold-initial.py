import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, lit, coalesce, count, sum as spark_sum, max as spark_max

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read Silver
df_items = spark.read.parquet("s3://gbp-202605-silver/order_items/")
df_options = spark.read.parquet("s3://gbp-202605-silver/order_item_options/")
df_options = df_options.drop("order_id") # need to drop this since items already has this same column
df_dates = spark.read.parquet("s3://gbp-202605-silver/date_dim/")

# adding this aggregrate options per line_item BEFORE joining -> discovered earlier that total revenue wasn't correct because item options are one row each
options_summary = df_options.groupBy("lineitem_id").agg(
  count("*").alias("option_count"),
  spark_sum(col("option_price") * col("option_quantity")).alias("option_revenue"),
  spark_max(when(col("option_price") < 0, 1).otherwise(0)).alias("is_discounted")
)

# Join them together
gold = df_items.join(df_dates, df_items.creation_date == df_dates.date_key, "left")
gold = gold.join(options_summary, "lineitem_id", "left")

# add a simple derived column called line_revenue(price*quantity)
gold = gold \
  .withColumn("item_revenue", col("item_price") * col("item_quantity")) \
  .withColumn("option_revenue", coalesce(col("option_revenue"), lit(0))) \
  .withColumn("total_line_revenue", col("item_revenue") + col("option_revenue"))

# Write to gold 
gold.write.mode("overwrite").parquet("s3://gbp-202605-gold/fact_order_items/")

job.commit()