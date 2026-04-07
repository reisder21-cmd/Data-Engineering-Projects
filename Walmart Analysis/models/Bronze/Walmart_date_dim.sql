{{
config
({
"materialized":'table',
"schema": 'BRONZE'
})
}}




WITH walmart_date_dim AS 
(
SELECT 
TO_CHAR(DATE, '%Y-%m-%d') AS DATE_ID,
DATE,
ISHOLIDAY,
CURRENT_TIMESTAMP(6) AS INSERT_DATE,
CURRENT_TIMESTAMP(6) AS UPDATE_DATE
FROM {{source('department_data','DIM_DEPARTMENT_RAW')}}
)

SELECT * FROM walmart_date_dim