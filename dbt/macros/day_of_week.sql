{% macro day_of_week_iso(expr) -%}
  {{ return(adapter.dispatch('day_of_week_iso', 'your_project')(expr)) }}
{%- endmacro %}

{% macro is_weekend(expr) -%}
  {{ return(adapter.dispatch('is_weekend', 'your_project')(expr)) }}
{%- endmacro %}

{% macro default__day_of_week_iso(expr) -%}
    cast(strftime({{ expr }}, '%u') as integer)
{%- endmacro %}

{% macro default__is_weekend(expr) -%}
    case
        when cast(strftime({{ expr }}, '%u') as integer) in (6, 7) then true
        else false
    end
{%- endmacro %}

{% macro duckdb__day_of_week_iso(expr) -%}
    cast(strftime({{ expr }}, '%u') as integer)
{%- endmacro %}

{% macro duckdb__is_weekend(expr) -%}
    case
        when cast(strftime({{ expr }}, '%u') as integer) in (6, 7) then true
        else false
    end
{%- endmacro %}


{% macro databricks__day_of_week_iso(expr) -%}
    cast(date_format({{ expr }}, 'u') as integer)
{%- endmacro %}

{% macro databricks__is_weekend(expr) -%}
    case
        when cast(date_format({{ expr }}, 'u') as integer) in (6, 7) then true
        else false
    end
{%- endmacro %}
