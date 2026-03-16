{{
config
({
"materialized":'table',
"pre_hook": copy_json('COUNTRY_DETAILS_CP'),
"schema": 'RAW'
})
}}

WITH country_details_raw AS
(
SELECT X.VALUE AS SOURCE_DATA,
CURRENT_TIMESTAMP(6) AS INSERT_DTS
FROM {{source('country','COUNTRY_DETAILS_CP')}} A,
LATERAL FLATTEN (A.DATA) X
)

SELECT
CAST(SOURCE_DATA AS VARIANT) AS SOURCE_DATA,
CAST(INSERT_DTS AS TIMESTAMP(6)) AS INSERT_DTS
FROM country_details_raw

