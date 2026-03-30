from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# creating a new SDP table under silver schema
@dp.table(
    name = "lakehouse.`03_silver`.sdp_earthquake_events_stream",
    comment = "Silver table for earthquake events from SDP"

)

#### COPY FROM SILVER LOAD ####
def sdp_earthquake_events_stream():
    source_data = spark.readStream.table("lakehouse.`02_bronze`.sdp_earthquake_source_data")

    silver_source = (
      source_data.select("id", "time", "longitude", "latitude", "depth", "mag", "magType", "place", "gap", "dmin", "rms", "net","code", "ids", "sources","types","nst","title","status","tsunami","sig","felt","updated")
     .dropDuplicates()
     .withColumn('hash_id', F.sha2(F.concat_ws('_', F.col('id'), F.col('time')),256))) # hash_id is used to deduplicate the data

    # Will not be using the windows function that we created in silver load, SDP pipeline should be able to do it on its own
    return silver_source

dp.create_streaming_table(name="lakehouse.`03_silver`.earthquake_data_final_with_cdc_stream")

dp.create_auto_cdc_flow(
    target = "lakehouse.`03_silver`.earthquake_data_final_with_cdc_stream",
    source = "lakehouse.`03_silver`.sdp_earthquake_events_stream",
    keys = ["hash_id"],
    sequence_by = F.col("updated"),
    stored_as_scd_type = 1
)