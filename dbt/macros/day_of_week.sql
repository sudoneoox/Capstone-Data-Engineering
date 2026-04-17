{% macro day_of_week_iso(expr) -%}
  {{ return(adapter.dispatch('day_of_week_iso')(expr)) }}
{%- endmacro %}

{% macro is_weekend(expr) -%}
  {{ return(adapter.dispatch('is_weekend')(expr)) }}
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
    weekday({{ expr }}) + 1
{%- endmacro %}

{% macro databricks__is_weekend(expr) -%}
    case
        when weekday({{ expr }}) + 1 in (6, 7) then true
        else false
    end
{%- endmacro %}
