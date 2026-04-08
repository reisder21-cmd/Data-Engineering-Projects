{{ config(
    materialized='incremental',
    unique_key='STORE_ID'
) }}

    SELECT
      STORE_ID,
      STORE_TYPE,
      STORE_SIZE,
      CURRENT_TIMESTAMP(6) AS UPDATE_DATE
    FROM {{ref('stage_store')}} 