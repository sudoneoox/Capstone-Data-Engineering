{% macro string_agg_sorted_distinct(expr, delimiter=", ") -%}
  {{ return(adapter.dispatch('string_agg_sorted_distinct')(expr, delimiter)) }}
{%- endmacro %}

{% macro duckdb__string_agg_sorted_distinct(expr, delimiter=", ") -%}
  string_agg(distinct {{ expr }}, '{{ delimiter }}' order by {{ expr }})
{%- endmacro %}

{% macro databricks__string_agg_sorted_distinct(expr, delimiter=", ") -%}
  array_join(
    array_sort(
      collect_set({{ expr }})
    ),
    '{{ delimiter }}'
  )
{%- endmacro %}
