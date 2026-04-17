{% macro col(name) -%}
  {{ return(adapter.dispatch('col')(name)) }}
{%- endmacro %}

{% macro duckdb__col(name) -%}
  "{{ name }}"
{%- endmacro %}

{% macro databricks__col(name) -%}
  `{{ name }}`
{%- endmacro %}
