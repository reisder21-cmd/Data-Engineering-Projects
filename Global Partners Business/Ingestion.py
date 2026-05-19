import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Fetch connection details from Glue Catalog
connection_name = "Jdbc connection to MSSQL"
jdbc_conf = glueContext.extract_jdbc_conf(connection_name)
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

# 2. Extract properties
url = jdbc_conf['url']
url = url + ";databaseName=GlobalPartners"
user = jdbc_conf['user']
password = jdbc_conf['password']


tables = ["order_items", "order_item_options", "date_dim"]

#loop through tables and read them
for table in tables:
  df = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", table) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", driver) \
    .load()

# write to S3
  df.write \
    .mode("overwrite") \
    .parquet(f"s3://gbp-202605-bronze/{table}/")

  print(f"{table} ingested")


job.commit()