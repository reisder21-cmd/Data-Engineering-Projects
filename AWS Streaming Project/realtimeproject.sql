USE ROLE ACCOUNTADMIN;

USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE DATABASE MYDB;

-- Setup storage integration

CREATE OR REPLACE STORAGE INTEGRATION S3_INT

  TYPE = EXTERNAL_STAGE

  STORAGE_PROVIDER = 'S3'

  ENABLED = TRUE

  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::012178638860:role/snowpiperole'

  STORAGE_ALLOWED_LOCATIONS = ('s3://realitimeproject-data-bucket/data/');
  

  -- Get values 5 and 7 to input into AWS IAM role trust relationship

  DESC INTEGRATION S3_INT;

  
-- Create the S3 stage environment

  CREATE OR REPLACE STAGE MYDB.PUBLIC.S3_STAGE

  STORAGE_INTEGRATION = S3_INT

  URL= 's3://realitimeproject-data-bucket/data/';
  

  -- Test connection to S3 below, should return no results at this point

  ls @MYDB.PUBLIC.S3_STAGE;


-- Create table and load as data variant for unstructered data coming in
  CREATE OR REPLACE TABLE MYDB.PUBLIC.MYTABLE
  (
    DATA VARIANT
  );


  -- Setup the  snowpipe to load data from S3 bucket to raw table created above

CREATE OR REPLACE PIPE MYDB.PUBLIC.MYPIPE AUTO_INGEST = TRUE AS

COPY INTO MYDB.PUBLIC.MYTABLE

FROM @MYDB.PUBLIC.S3_STAGE

FILE_FORMAT = (TYPE = 'JSON');

USE SCHEMA MYDB.PUBLIC;

-- Run this to get  notification_channel property for SQS. Then go to S3 Event Notification and setup for folder/ in bucket with SQS for destination
SHOW PIPES;

SELECT * FROM MYDB.PUBLIC.MYTABLE;
