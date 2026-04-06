{{
config
({
"materialized":'table',
"schema": 'BRONZE'
})
}}

WITH store_raw AS (
    SELECT
    STORE AS STORE_ID,
    TYPE AS STORE_TYPE,
    SIZE AS STORE_SIZE,
    CURRENT_TIMESTAMP(6) AS INSERT_DATE,
    CURRENT_TIMESTAMP(6) AS UPDDATE_DATE
    FROM {{source('store_data','DIM_STORES_RAW')}}
)

SELECT * FROM store_raw