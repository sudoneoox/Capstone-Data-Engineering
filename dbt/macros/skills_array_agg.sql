{% macro skills_array_agg(expr, distinct=true) -%}
  {{ return(adapter.dispatch('skills_array_agg', 'your_project')(expr, distinct)) }}
{%- endmacro %}

{% macro duckdb__skills_array_agg(expr, distinct=true) -%}
    {%- if distinct -%}
        list(distinct {{ expr }}) filter (where {{ expr }} is not null)
    {%- else -%}
        list({{ expr }}) filter (where {{ expr }} is not null)
    {%- endif -%}
{%- endmacro %}

{% macro databricks__skills_array_agg(expr, distinct=true) -%}
    {%- if distinct -%}
        collect_set({{ expr }})
    {%- else -%}
        collect_list({{ expr }})
    {%- endif -%}
{%- endmacro %}
