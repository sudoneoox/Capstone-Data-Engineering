{% macro epoch_ms_to_timestamp(expr) -%}
  {{ return(adapter.dispatch('epoch_ms_to_timestamp')(expr)) }}
{%- endmacro %}

{% macro duckdb__epoch_ms_to_timestamp(expr) -%}
  epoch_ms({{ expr }})
{%- endmacro %}

{% macro databricks__epoch_ms_to_timestamp(expr) -%}
  to_timestamp(from_unixtime(cast({{ expr }} / 1000 as bigint)))
{%- endmacro %}
