{% macro explode_csv_from(relation_alias, column_name, alias_name='skill') -%}
  {{ return(adapter.dispatch('explode_csv_from')(relation_alias, column_name, alias_name)) }}
{%- endmacro %}

{% macro duckdb__explode_csv_from(relation_alias, column_name, alias_name='skill') -%}
{{ relation_alias }},
unnest(string_split({{ column_name }}, ',')) AS _u({{ alias_name }})
{%- endmacro %}

{% macro databricks__explode_csv_from(relation_alias, column_name, alias_name='skill') -%}
{{ relation_alias }}
LATERAL VIEW explode(split({{ column_name }}, ',')) exploded_{{ alias_name }} AS {{ alias_name }}
{%- endmacro %}
