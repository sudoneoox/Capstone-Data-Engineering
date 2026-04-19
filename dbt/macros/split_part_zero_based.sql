{% macro split_part_zero_based(expr, delimiter, index) -%}
  {{ return(adapter.dispatch('split_part_zero_based')(expr, delimiter, index)) }}
{%- endmacro %}

{% macro duckdb__split_part_zero_based(expr, delimiter, index) -%}
  split({{ expr }}, {{ delimiter }})[{{ index + 1 }}]
{%- endmacro %}

{% macro databricks__split_part_zero_based(expr, delimiter, index) -%}
  get(split({{ expr }}, {{ delimiter }}), {{ index }})
{%- endmacro %}
