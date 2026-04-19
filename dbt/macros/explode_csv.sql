{% macro explode_csv(column_name, alias_name='skill') -%}
  {{ return(adapter.dispatch('explode_csv')(column_name, alias_name)) }}
{%- endmacro %}

{% macro duckdb__explode_csv(column_name, alias_name='skill') -%}
  unnest(string_split({{ column_name }}, ',')) as {{ alias_name }}
{%- endmacro %}

{% macro databricks__explode_csv(column_name, alias_name='skill') -%}
  explode(split({{ column_name }}, ',')) as {{ alias_name }}
{%- endmacro %}
