{% macro safe_numeric_cast(expr, target_type) -%}
  {{ return(adapter.dispatch('safe_numeric_cast')(expr, target_type)) }}
{%- endmacro %}

{% macro duckdb__safe_numeric_cast(expr, target_type) -%}
  try_cast({{ expr }} as {{ target_type }})
{%- endmacro %}

{% macro databricks__safe_numeric_cast(expr, target_type) -%}
  try_cast({{ expr }} as {{ target_type }})
{%- endmacro %}
