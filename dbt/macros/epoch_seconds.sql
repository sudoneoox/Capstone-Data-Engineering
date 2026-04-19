{% macro epoch_seconds_to_timestamp(expr) -%}
  {{ return(adapter.dispatch('epoch_seconds_to_timestamp')(expr)) }}
{%- endmacro %}

{% macro duckdb__epoch_seconds_to_timestamp(expr) -%}
  to_timestamp({{ expr }})
{%- endmacro %}

{% macro databricks__epoch_seconds_to_timestamp(expr) -%}
  to_timestamp(from_unixtime(cast({{ expr }} as bigint)))
{%- endmacro %}
