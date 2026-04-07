{{
config
({
"materialized":'table',
"schema": 'BRONZE'
})
}}

WITH store AS (
    SELECT
    STORE AS STORE_ID,
    TYPE AS STORE_TYPE,
    SIZE AS STORE_SIZE
    FROM {{source('store_data','DIM_STORES_RAW')}}
),
dept AS (
    SELECT
    STORE AS STORE_ID,
    DEPT AS DEPT_ID
    FROM {{source('department_data','DIM_DEPARTMENT_RAW')}}
)

SELECT
    s.STORE_ID,
    d.DEPT_ID,
    s.STORE_TYPE,
    s.STORE_SIZE,
    CURRENT_TIMESTAMP(6) AS INSERT_DATE,
    CURRENT_TIMESTAMP(6) AS UPDDATE_DATE
FROM store s
LEFT JOIN dept d 
ON s.STORE_ID = d.STORE_ID

