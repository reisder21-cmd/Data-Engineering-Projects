{% snapshot product_snapshot %}

{{

    config(

      target_database='MYDB',

      target_schema='snapshots',

      unique_key='PRODUCT_ID',

      strategy='check',

      check_cols=['PRODUCT_NAME', 'CATEGORY', 'SELLING_PRICE', 'MODEL_NUMBER', 'ABOUT_PRODUCT', 'PRODUCT_SPECIFICATION', 

                  'TECHNICAL_DETAILS', 'SHIPPING_WEIGHT', 'PRODUCT_DIMENSIONS'],

    )

}}

select * from {{ source('xfm', 'WORK_PRODUCT_TRANSFORM') }}

{% endsnapshot %}
