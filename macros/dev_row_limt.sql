{% macro dev_limit_rows(row_count) -%}

{# set row count limit on base tables for dev runs - '1000' is default value if not explicitly defined #}

{% if target.name == 'dev' %}

  limit {{ var("row_count", "1000") }}

{% endif %}

{%- endmacro %}