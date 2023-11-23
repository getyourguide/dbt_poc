{%- macro get_country_id(country_name) %}
    {%- set get_country_id_query %}
    SELECT
        distinct country_id
    FROM {{ source('dwh','dim_country') }}
    WHERE lower(country_name) = lower('{{country_name}}')
    LIMIT 1
    {%- endset %}
    {%- if execute %}
    {%- set results = dbt_utils.get_single_value(get_country_id_query,-1)  %}
    {%- else %}
    {%- set results = -1 %}
    {%- endif %}
    {{ results }}
{%- endmacro %}