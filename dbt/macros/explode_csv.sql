
{% macro explode_csv(column_name) %}
  {{ return(adapter.dispatch('explode_csv')(column_name)) }}
{% endmacro %}

{% macro default__explode_csv(column_name) %}
  LATERAL (SELECT UNNEST(STRING_SPLIT({{ column_name }}, ',')) AS skill)
{% endmacro %}

{% macro duckdb__explode_csv(column_name) %}
  LATERAL (SELECT UNNEST(STRING_SPLIT({{ column_name }}, ',')) AS skill)
{% endmacro %}

{% macro databricks__explode_csv(column_name) %}
  LATERAL VIEW explode(split({{ column_name }}, ',')) exploded_skills AS skill
{% endmacro %}
