-- SCD1

{{ config(
    materialized='incremental',
    unique_key='ROW_NUM'
) }}

SELECT
    ROW_NUM,
    DATE_ID,
    STORE_DATE,
    ISHOLIDAY,
    CURRENT_TIMESTAMP(6) AS UPDATE_DATE
FROM {{ ref('stage_dept_date') }}
