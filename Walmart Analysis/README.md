## Walmart Analysis Project

#### 1. Three raw csv files from S3 bucket are ingested into tables in snowflake bronze schema.
#### 2. In DBT, staging is completed in Silver layer to clean any data from Bronze. 
#### 3. For SCD1, 2 dim tables are created in gold layer from the staging tables in silver
#### 4. For SCD2, an intermediate table was created in silver layer to join other tables to get all data that was required for final fact table. 
#### 5. Continuing with SCD2 configuration, a snapshot file was created using a surrogate key to track changes
#### 6. Final fact table in gold layer, references the snapshot to track changes to fact table
#### 7. With the final data in gold layer, I was able to create dashboards using Tableau. Look under Dashboards to see document with dashboards.
