USE ROLE ACCOUNTADMIN;

USE WAREHOUSE compute_wh;

CREATE OR REPLACE DATABASE GLUEDB;

-- Create storage integration

CREATE OR REPLACE STORAGE INTEGRATION GLUE_S3_INT

  TYPE = EXTERNAL_STAGE

  STORAGE_PROVIDER = 'S3'

  ENABLED = TRUE

  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::012178638860:role/snowflakerole'

  STORAGE_ALLOWED_LOCATIONS = ('s3://de-glueproject/data/');

  -- Create trust between Snowflake and AWS

  DESC INTEGRATION GLUE_S3_INT;


-- Create the external stage with S3

CREATE OR REPLACE STAGE GLUEDB.PUBLIC.GLUE_S3_STAGE

STORAGE_INTEGRATION = GLUE_S3_INT

URL= 's3://de-glueproject/data/';


-- list contents of S3 bucket to verify working
ls @GLUEDB.PUBLIC.GLUE_S3_STAGE;

-- Create a copy table to get raw data from S3

CREATE OR REPLACE TABLE GLUEDB.PUBLIC.COUNTRY_DETAILS_CP
(
DATA VARIANT
);

SELECT * FROM GLUEDB.PUBLIC.COUNTRY_DETAILS_CP;

SELECt * FROM GLUEDB.RAW.COUNTRY_DETAILS_RAW;

SELECT * FROM GLUEDB.TRANSFORM.COUNTRY_DETAILS_TRANSFORM;


-- Get all the contient names from our trasnform table
SELECT DISTINCT COUNTRY_CONTINENT_NAME
FROM GLUEDB.TRANSFORM.COUNTRY_DETAILS_TRANSFORM;

SELECT * FROM GLUEDB.MART.COUNTRY_DETAILS_AFRICA;

-- Create a production database for our DBT job
CREATE OR REPLACE DATABASE GLUEDB_PRODUCTION;

SELECT 
    table_name, 
    row_count
FROM 
    information_schema.tables 
WHERE 
    table_catalog = 'GLUEDB_PRODUCTION'
    AND table_type = 'BASE TABLE'
ORDER BY 

    table_name;
