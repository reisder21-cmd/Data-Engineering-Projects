from airflow import DAG

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import datetime

DEFAULT_ARGS = {

    'owner': 'airflow',

    'start_date': datetime(2024, 1, 1),

    'retries': 0

}

with DAG(

    dag_id='test_snowflake_connection',

    default_args=DEFAULT_ARGS,

    schedule=None,  # Trigger manually

    catchup=False,

    description='A simple DAG to test Snowflake connection',

    tags=['test', 'snowflake']

) as dag:

    test_snowflake_connection = SQLExecuteQueryOperator(

        task_id='run_test_query',

        conn_id='snowflake_conn',  # Update this to match your Airflow connection ID

 

sql="""

            USE WAREHOUSE COMPUTE_WH;

            USE DATABASE AIRFLOW_DB;

            USE SCHEMA BRONZE;

            SELECT CURRENT_TIMESTAMP;

        """

    )