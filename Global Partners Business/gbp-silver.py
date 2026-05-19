import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit,col, sequence, explode, month, weekofyear,date_format,dayofweek,when, to_date,to_timestamp, substring, year ## Need this to do transformations
from pyspark.sql import functions as F

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Functions for each table in bronze with transformations
# def transform_date_dim(df):
#     df = df.withColumn("is_holiday", when(col("is_holiday") == "TRUE", True).otherwise(False))
#     df = df.withColumn("is_weekend", when(col("is_weekend") == "TRUE", True).otherwise(False))
#     df = df.withColumn("date_key", to_date(col("date_key"), "MM-dd-yyyy")) # This doesn't work based on the bad raw-data, need to create a complete 2023 calendar
#     return df
def transform_date_dim(_df): # this df is there but its being ignored because we are creating our own transformation table
    # Generate complete 2023 calendar
    full_calendar = spark.sql("""
        SELECT explode(sequence(to_date('2023-01-01'), to_date('2023-12-31'), interval 1 day)) as date_key
    """)
    # Derive calendar attributes from date_key
    full_calendar = full_calendar \
        .withColumn("year", year("date_key")) \
        .withColumn("month", month("date_key")) \
        .withColumn("week", weekofyear("date_key")) \
        .withColumn("day_of_week", dayofweek("date_key").isin(1, 7)) \
        .withColumn("is_holiday", lit(False))
    return full_calendar
def transform_order_items(df):
    df = df.dropna(subset=['lineitem_id'])
    df = df.withColumn("isloyalty", when(col("is_loyalty") == "TRUE", True).otherwise(False))
    #df = df.withColumn("creation_time_utc", F.to_timestamp("creation_time_utc"))
    df = df.withColumn("creation_time_utc", to_timestamp(substring(col("creation_time_utc"),1,19), "yyyy-MM-dd'T'HH:mm:ss"))
    df = df.withColumn("creation_date", to_date(col("creation_time_utc")))
    df = df.filter(year(col("creation_date")) == 2023) # date dim only has 2023,so only going to use these dates for scope of project
    return df
def transform_order_options(df):
    df = df.dropDuplicates()
    return df

# Map table names to their transformation functions, so we can loop through instead of writing so many lines
transformations = {
    "date_dim":           transform_date_dim,
    "order_items":        transform_order_items,
    "order_item_options": transform_order_options,
}



# Main Flow
for table_name,transform_fn in transformations.items():
  df = spark.read.parquet(f"s3://gbp-202605-bronze/{table_name}/")
  df = transform_fn(df)
  df.write.mode("overwrite").parquet(f"s3://gbp-202605-silver/{table_name}/")
 
job.commit()