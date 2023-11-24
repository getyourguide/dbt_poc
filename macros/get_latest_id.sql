{% macro get_latest_id(timestamp_col, id_col) %}

        split(max({{ timestamp_col }} || '#' || {{ id_col }}), '#')[1] as {{id_col}}

{% endmacro %}