{#
  Resolve the path to a Parquet source file based on the dbt target.

  Usage in a model:
    SELECT * FROM {{ parquet_path('adzuna', 'adzuna_data_engineer_all.parquet') }}

  Dev (DuckDB):   read_parquet('data/parquet/adzuna/adzuna_data_engineer_all.parquet')
  Prod (Databricks): read_files('gs://bucket/raw/adzuna/adzuna_data_engineer_all.parquet')
#}

{% macro parquet_path(source_name, file_name) %}
  {% if target.type == 'duckdb' %}
    read_parquet('data/parquet/{{ source_name }}/{{ file_name }}')
  {% else %}
    read_files('gs://{{ var("gcs_bucket") }}/raw/{{ source_name }}/{{ file_name }}')
  {% endif %}
{% endmacro %}
