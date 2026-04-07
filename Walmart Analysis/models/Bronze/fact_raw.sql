{{
config
({
"materialized":'table',
"schema": 'BRONZE'
})
}}

WITH fact_raw AS (
    SELECT
        STORE AS STORE_ID,
        DATE,
        TEMPERATURE AS STORE_TEMPERATURE,
        FUEL_PRICE,
        MARKDOWN1,
        MARKDOWN2,
        MARKDOWN3,
        MARKDOWN4,
        MARKDOWN5,
        CPI,
        UNEMPLOYMENT,
        ISHOLIDAY,
        CURRENT_TIMESTAMP(6) AS INSERT_DATE,
        CURRENT_TIMESTAMP(6) AS UPDATE_DATE,
        CURRENT_TIMESTAMP(6) AS VRSN_START_DATE,
        CURRENT_TIMESTAMP(6) AS VRSN_END_DATE
    FROM {{source('fact_data','FACT_RAW')}}
)

SELECT * FROM fact_raw