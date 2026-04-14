
{% macro unpivot_fred_series(relation, date_column, series_columns) %}
  {{ return(adapter.dispatch('unpivot_fred_series')(relation, date_column, series_columns)) }}
{% endmacro %}

{% macro duckdb__unpivot_fred_series(relation, date_column, series_columns) %}
    UNPIVOT {{ relation }}
    ON
    {%- for col in series_columns %}
        "{{ col }}"{% if not loop.last %}, {% endif %}
    {%- endfor %}
    INTO
        NAME series_id
        VALUE value
{% endmacro %}

{% macro default__unpivot_fred_series(relation, date_column, series_columns) %}
    {{ dbt.exceptions.raise_compiler_error(
        "unpivot_fred_series is not implemented for adapter: " ~ target.type
    ) }}
{% endmacro %}

{% macro databricks__unpivot_fred_series(relation, date_column, series_columns) %}
(
    {%- for col in series_columns %}
    SELECT
        {{ date_column }} AS {{ date_column }},
        '{{ col }}' AS series_id,
        {{ col }} AS value
    FROM {{ relation }}
    {%- if not loop.last %}

    UNION ALL

    {%- endif %}
    {%- endfor %}
)
{% endmacro %}
