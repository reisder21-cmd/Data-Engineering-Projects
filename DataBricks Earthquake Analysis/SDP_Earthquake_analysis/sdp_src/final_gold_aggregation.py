from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import *

@dp.table(
    name = 'lakehouse.`04_gold`.sdp_geospatial_hotspot',
    comment = "gold table for sdp"
)

def sdp_geospatial_hotspot():
    source_df = spark.read.table('lakehouse.`03_silver`.earthquake_data_final_with_cdc_stream')

# FROM GOLD  geo_spatial_hotspot
    event_density = (
    source_df
    .groupBy(
        F.round(F.col('latitude'), 1).alias('avg_latitude'),
        F.round(F.col('longitude'), 1).alias('avg_longitude'),
    ) 
    .agg(
        F.count("*").alias("event_density"),
        F.avg("mag").alias("avg_mag"),
        F.max("mag").alias("max_mag")
    )
     .withColumn('hash_id', F.sha2(F.concat_ws('_', F.col('avg_latitude'), F.col('avg_longitude')),256).substr(0,15))

    )

    return event_density

@dp.table(
    name = 'lakehouse.`04_gold`.sdp_seismic_activity',
    comment = 'gold table for sdp on seismic activity'
)

def sdp_seismic_activity():
    source_df = spark.read.table('lakehouse.`03_silver`.earthquake_data_final_with_cdc_stream')

# FROM GOLD  seismic_activity
    seismic_activity = (
     source_df
         .withColumn("activity_hour", F.date_trunc("hour", F.col("time"))) # -> new column called activity_hour,where hour is the same for a group of events
         .groupBy("activity_hour","net") # -> group by hour and net(all the locations)
         .agg(
            F.count("hash_id").alias("total_event"),
            F.avg("mag").alias("avg_mag"),
            F.max("mag").alias("max_mag")
            )
            .withColumn('hash_id', F.sha2(F.concat_ws('_', F.col('activity_hour'), F.col('net')),256).substr(0,15)) # -> using substring to shorten the hash
                                                      )
    return seismic_activity