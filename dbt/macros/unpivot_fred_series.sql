{% macro unpivot_fred_series(relation, date_column, series_columns) %}
  {{ return(adapter.dispatch('unpivot_fred_series')(relation, date_column, series_columns)) }}
{% endmacro %}

{% macro duckdb__unpivot_fred_series(relation, date_column, series_columns) %}
(
    {%- for col_name in series_columns %}
    SELECT
        {{ date_column }} AS date_raw,
        '{{ col_name }}' AS series_id,
        "{{ col_name }}" AS value
    FROM {{ relation }}
    {%- if not loop.last %}

    UNION ALL

    {%- endif %}
    {%- endfor %}
)
{% endmacro %}

{% macro default__unpivot_fred_series(relation, date_column, series_columns) %}
    {{ dbt.exceptions.raise_compiler_error(
        "unpivot_fred_series is not implemented for adapter: " ~ target.type
    ) }}
{% endmacro %}

{% macro databricks__unpivot_fred_series(relation, date_column, series_columns) %}
(
    {%- for col_name in series_columns %}
    SELECT
        {{ date_column }} AS date_raw,
        '{{ col_name }}' AS series_id,
        {{ col_name }} AS value
    FROM {{ relation }}
    {%- if not loop.last %}

    UNION ALL

    {%- endif %}
    {%- endfor %}
)
{% endmacro %}
