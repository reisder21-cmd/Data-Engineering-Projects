{% macro macros_copy_csv(table_nm) %} 

 

delete from {{var ('rawhist_db') }}.{{var ('wrk_schema')}}.{{ table_nm }};

 

COPY INTO {{var ('rawhist_db') }}.{{var ('wrk_schema')}}.{{ table_nm }} 

FROM 

(

SELECT

    $1 AS ProductId,

    $2 AS ProductName,

    $3 AS Category,

    $4 AS SellingPrice,

    $5 AS ModelNumber,

    $6 AS AboutProduct,

    $7 AS ProductSpecification,

    $8 AS TechnicalDetails,

    $9 AS ShippingWeight,

    $10 AS ProductDimensions,

    CURRENT_TIMESTAMP() AS INSERT_DTS,

    CURRENT_TIMESTAMP() AS UPDATE_DTS,

    metadata$filename AS SOURCE_FILE_NAME,

    metadata$file_row_number AS SOURCE_FILE_ROW_NUMBER

FROM @{{ var('stage_name') }}

)

FILE_FORMAT = {{var ('file_format_json') }}

PURGE={{ var('purge_status') }}

FORCE = TRUE

;

 

{% endmacro %}
