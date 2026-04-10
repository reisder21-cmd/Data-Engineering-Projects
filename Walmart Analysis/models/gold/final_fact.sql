SELECT *
FROM {{ ref('walmart_fact_snapshot') }}
WHERE DBT_VALID_TO IS NULL