{% macro date_format_crossdb(expr, fmt_key) %}
    {{ return(adapter.dispatch('date_format_crossdb')(expr, fmt_key)) }}
{% endmacro %}

{% macro duckdb__date_format_crossdb(expr, fmt_key) -%}
    {%- set format_map = {
        'date_key': '%Y%m%d',
        'month_name': '%B'
    } -%}

    strftime({{ expr }}, '{{ format_map[fmt_key] }}')
{%- endmacro %}

{% macro databricks__date_format_crossdb(expr, fmt_key) -%}
    {%- set format_map = {
        'date_key': 'yyyyMMdd',
        'month_name': 'MMMM'
    } -%}

    date_format({{ expr }}, '{{ format_map[fmt_key] }}')
{%- endmacro %}


{% macro default__date_format_crossdb(expr, fmt_key) -%}
    {%- set format_map = {
        'date_key': '%Y%m%d',
        'month_name': '%B'
    } -%}

    strftime({{ expr }}, '{{ format_map[fmt_key] }}')
{%- endmacro %}
