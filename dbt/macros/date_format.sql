{% macro date_format_crossdb(date_expr, format_name) -%}
  {{ return(adapter.dispatch('date_format_crossdb')(date_expr, format_name)) }}
{%- endmacro %}

{% macro duckdb__date_format_crossdb(date_expr, format_name) -%}
  {%- if format_name == 'date_key' -%}
    strftime({{ date_expr }}, '%Y%m%d')
  {%- elif format_name == 'month_name' -%}
    strftime({{ date_expr }}, '%B')
  {%- else -%}
    {{ exceptions.raise_compiler_error("Unsupported format_name for duckdb: " ~ format_name) }}
  {%- endif -%}
{%- endmacro %}

{% macro databricks__date_format_crossdb(date_expr, format_name) -%}
  {%- if format_name == 'date_key' -%}
    date_format({{ date_expr }}, 'yyyyMMdd')
  {%- elif format_name == 'month_name' -%}
    date_format({{ date_expr }}, 'MMMM')
  {%- else -%}
    {{ exceptions.raise_compiler_error("Unsupported format_name for databricks: " ~ format_name) }}
  {%- endif -%}
{%- endmacro %}
