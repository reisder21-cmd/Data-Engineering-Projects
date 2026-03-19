from airflow import DAG

from airflow.operators.python import PythonOperator

from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow.operators.empty import EmptyOperator 

import requests

import boto3

import os

 
# Configuration

GITHUB_API_URL = "https://api.github.com/repos/deacademygit/project-data/contents/healthcare"

RAW_BASE_URL = "https://raw.githubusercontent.com/deacademygit/project-data/main/healthcare"

S3_BUCKET = "dea-airflow-databucket"  # Replace with your actual S3 bucket

LOCAL_TMP_DIR = "/tmp/github_data"

 
def download_csvs_from_github_and_upload_to_s3():

    os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

    s3 = boto3.client("s3")

 
    response = requests.get(GITHUB_API_URL)

    response.raise_for_status()

    file_list = response.json()

 
    for file in file_list:

        if file['name'].endswith('.csv'):

            raw_url = f"{RAW_BASE_URL}/{file['name']}"

            local_path = os.path.join(LOCAL_TMP_DIR, file['name'])

            s3_key = file['name']

 
            print(f"Downloading {raw_url}")

            r = requests.get(raw_url)

            r.raise_for_status()

 
            with open(local_path, 'wb') as f:

                f.write(r.content)

 
            print(f"Uploading to S3: s3://{S3_BUCKET}/{s3_key}")

            s3.upload_file(local_path, S3_BUCKET, s3_key)

 
default_args = {

    'owner': 'airflow',

    'start_date': datetime(2024, 1, 1),

    'retries': 0,

}

 
with DAG(

    dag_id="healthcare_elt_pipeline",

    default_args=default_args,

    schedule=None,

    catchup=False,

    tags=["github", "s3", "csv", "snowflake"],

) as dag:

 
    # The DAG steps to execute the process to copy data from http url and upload in the S3 bucket

    download_and_upload = PythonOperator(

        task_id="upload_csvs_github_to_s3",

        python_callable=download_csvs_from_github_and_upload_to_s3,

    )

    

    # The DAG steps to execute the Bronze layer copy commands

 
    run_doctors_raw_load = SQLExecuteQueryOperator(

        task_id="truncate_and_copy_doctors_raw_table",

        conn_id="snowflake_conn",  # Update this to your Airflow Snowflake connection ID

        sql="""

        TRUNCATE TABLE AIRFLOW_DB.BRONZE.DOCTORS_RAW;

 
        COPY INTO AIRFLOW_DB.BRONZE.DOCTORS_RAW

        FROM @AIRFLOW_S3_STAGE/doctors.csv

        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)

        FORCE = TRUE;

        """

    )

 
    run_hospitals_raw_load = SQLExecuteQueryOperator(

        task_id="truncate_and_copy_hospitals_raw_table",

        conn_id="snowflake_conn",  # Update this to your Airflow Snowflake connection ID

        sql="""

        TRUNCATE TABLE AIRFLOW_DB.BRONZE.HOSPITALS_RAW;

 
        COPY INTO AIRFLOW_DB.BRONZE.HOSPITALS_RAW

        FROM @AIRFLOW_S3_STAGE/hospitals.csv

        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)

        FORCE = TRUE;

        """

    )

 
    run_patients_raw_load = SQLExecuteQueryOperator(

        task_id="truncate_and_copy_patients_raw_table",

        conn_id="snowflake_conn",  # Update this to your Airflow Snowflake connection ID

        sql="""

        TRUNCATE TABLE AIRFLOW_DB.BRONZE.PATIENTS_RAW;

 
        COPY INTO AIRFLOW_DB.BRONZE.PATIENTS_RAW

        FROM @AIRFLOW_S3_STAGE/patients.csv

        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)

        FORCE = TRUE;

        """

    )

 
    run_treatments_raw_load = SQLExecuteQueryOperator(

        task_id="truncate_and_copy_treatments_raw_table",

        conn_id="snowflake_conn",  # Update this to your Airflow Snowflake connection ID

        sql="""

        TRUNCATE TABLE AIRFLOW_DB.BRONZE.TREATMENTS_RAW;

 
        COPY INTO AIRFLOW_DB.BRONZE.TREATMENTS_RAW

        FROM @AIRFLOW_S3_STAGE/treatments.csv

        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)

        FORCE = TRUE;

        """

    )

 
    run_visitis_raw_load = SQLExecuteQueryOperator(

        task_id="truncate_and_copy_visits_raw_table",

        conn_id="snowflake_conn",  # Update this to your Airflow Snowflake connection ID

        sql="""

        TRUNCATE TABLE AIRFLOW_DB.BRONZE.VISITS_RAW;

 
        COPY INTO AIRFLOW_DB.BRONZE.VISITS_RAW

        FROM @AIRFLOW_S3_STAGE/visits.csv

        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)

        FORCE = TRUE;

        """

    )

 
    raw_loads_complete = EmptyOperator(task_id="raw_tables_loads_complete")

 
    # The DAG steps to execute the Silver layer stored procedures

    run_patients_transform_sp = SQLExecuteQueryOperator(

        task_id="run_patients_transform_sp",

        conn_id="snowflake_conn",

        sql="CALL AIRFLOW_DB.SILVER.PATIENTS_TRANSFORM_SP();"

    )

 
    run_treatments_transform_sp = SQLExecuteQueryOperator(

        task_id="run_treatments_transform_sp",

        conn_id="snowflake_conn",

        sql="CALL AIRFLOW_DB.SILVER.TREATMENTS_TRANSFORM_SP();"

    )

 
    run_visits_transform_sp = SQLExecuteQueryOperator(

        task_id="run_visits_transform_sp",

        conn_id="snowflake_conn",

        sql="CALL AIRFLOW_DB.SILVER.VISITS_TRANSFORM_SP();"

    )

 
    transform_load_complete = EmptyOperator(task_id="transform_tables_loads_complete")

 
    # The DAG steps to execute the Gold layer stored procedures

    run_city_health_access_agg_sp = SQLExecuteQueryOperator(

        task_id="run_city_health_access_agg_sp",

        conn_id="snowflake_conn",

        sql="CALL AIRFLOW_DB.GOLD.CITY_HEALTHCARE_ACCESS_AGG_SP();"

    )

 
    run_hospital_performance_agg_sp = SQLExecuteQueryOperator(

        task_id="run_hospital_performance_agg_sp",

        conn_id="snowflake_conn",

        sql="CALL AIRFLOW_DB.GOLD.HOSPITAL_PERFORMANCE_AGG_SP();"

    )

 
    run_patient_value_agg_sp = SQLExecuteQueryOperator(

        task_id="run_patient_value_agg_sp",

        conn_id="snowflake_conn",

        sql="CALL AIRFLOW_DB.GOLD.PATIENT_VALUE_AGG_SP();"

    )

 
    run_patient_visit_summary_agg_sp = SQLExecuteQueryOperator(

        task_id="run_patient_visit_summary_agg_sp",

        conn_id="snowflake_conn",

        sql="CALL AIRFLOW_DB.GOLD.PATIENT_VISIT_SUMMARY_AGG_SP();"

    )

 
    agg_load_complete = EmptyOperator(task_id="agg_tables_loads_complete")

    

    # Run all raw loads in parallel after the download

    download_and_upload >> [run_doctors_raw_load, run_hospitals_raw_load, run_patients_raw_load, run_treatments_raw_load, run_visitis_raw_load]

 
    # All raw loads must finish before continuing

    [run_doctors_raw_load, run_hospitals_raw_load, run_patients_raw_load, run_treatments_raw_load, run_visitis_raw_load] >> raw_loads_complete

 
    # Continue to Silver Layer SPs

    raw_loads_complete >> [run_patients_transform_sp, run_treatments_transform_sp, run_visits_transform_sp]

 
    # All silver loads must finish before continuing

    [run_patients_transform_sp, run_treatments_transform_sp, run_visits_transform_sp] >> transform_load_complete

 
    # Continue to Gold Layer SPs

    transform_load_complete >> [run_city_health_access_agg_sp,run_hospital_performance_agg_sp,run_patient_value_agg_sp,run_patient_visit_summary_agg_sp] >> agg_load_complete