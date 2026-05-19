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
jdbc_url = "jdbc:sqlserver://dbdea-gpb.c8d4qe4sexxl.us-east-1.rds.amazonaws.com:1433;databaseName=GlobalPartners"
db_user = "admin"
db_password = "rwSm2DqYbJczkhFfVoGk"
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

df_items = spark.read.format("jdbc") \
    .option("url",jdbc_url) \
    .option("dbtable", "date_dim") \
    .option("user", db_user) \
    .option("driver", driver) \
    .option("password",db_password) \
    .load()
print("=== Schema of order_items ===")
df_items.printSchema()
print("=== First 5 rows ===")
df_items.show(5, truncate=False)

row_count = df_items.count()
print(f"=== Total row count: {row_count} ===")


job.commit()