{% macro inline_mapping_2col(rows, col1_name='series_id', col2_name='series_name') -%}
  {{ return(adapter.dispatch('inline_mapping_2col')(rows, col1_name, col2_name)) }}
{%- endmacro %}

{% macro duckdb__inline_mapping_2col(rows, col1_name='series_id', col2_name='series_name') -%}
(
  select *
  from (
    values
    {%- for row in rows %}
      ('{{ row[0] }}', '{{ row[1] }}'){% if not loop.last %},{% endif %}
    {%- endfor %}
  ) as t({{ col1_name }}, {{ col2_name }})
)
{%- endmacro %}

{% macro databricks__inline_mapping_2col(rows, col1_name='series_id', col2_name='series_name') -%}
(
  {%- for row in rows %}
    select '{{ row[0] }}' as {{ col1_name }}, '{{ row[1] }}' as {{ col2_name }}
    {%- if not loop.last %} union all {% endif %}
  {%- endfor %}
)
{%- endmacro %}
