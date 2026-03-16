{% macro copy_json(table_nm) %}

 

--Delete the data from the copy table before running the copy command

delete from {{var ('target_db') }}.{{var ('target_schema')}}.{{ table_nm }};

--Copy the data from the snowflake external stage to snowflake table

COPY INTO {{var ('target_db') }}.{{var ('target_schema')}}.{{ table_nm }}
FROM
(
SELECT
$1 AS DATA
FROM @{{ var('stage_name') }}
)
FILE_FORMAT = (TYPE = JSON)
FORCE = TRUE;

 
{% endmacro %}
