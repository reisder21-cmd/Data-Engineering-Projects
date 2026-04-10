{% snapshot walmart_fact_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='COMPOSITE_KEY',
        strategy='check',
        check_cols=[
            'STORE_WEEKLY_SALES',
            'FUEL_PRICE',
            'TEMPERATURE',
            'CPI',
            'UNEMPLOYMENT'
        ]
    )
}}

SELECT
    *,

    {{ dbt_utils.generate_surrogate_key([
        'STORE_ID',
        'DEPT_ID',
        'DATE_ID'
    ]) }} AS COMPOSITE_KEY

FROM {{ ref('fact_data') }}

{% endsnapshot %}