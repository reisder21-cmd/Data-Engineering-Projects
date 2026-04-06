{{
config
({
"materialized":'table',
"schema": 'BRONZE'
})
}}




WITH department_raw AS 
(
SELECT 
STORE AS STORE_ID,
DEPT AS DEPT_ID,
DATE,
WEEKLY_SALES,
ISHOLIDAY,
CURRENT_TIMESTAMP(6) AS INSERT_DATE,
CURRENT_TIMESTAMP(6) AS UPDDATE_DATE
FROM {{source('department_data','DIM_DEPARTMENT_RAW')}}
)

SELECT * FROM department_raw