WITH raw_fact AS (
    SELECT
        STORE AS STORE_ID,
        TO_CHAR(DATE, '%Y-%m-%d') AS DATE_ID,
        TEMPERATURE,
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
        CURRENT_TIMESTAMP(6) AS VRSN_START_DATE,
        CURRENT_TIMESTAMP(6) AS VRSN_END_DATE
    FROM {{source('bronze_data','FACT_RAW')}}
)

SELECT * FROM raw_fact