{% macro parquet_path(source_name, file_name) %}

  {% if target.type == 'duckdb' %}
    read_parquet('data/parquet/{{ source_name }}/{{ file_name }}')
  {% else %}
    read_files('gs://{{ var("gcs_bucket") }}/landing{{ source_name }}/{{ file_name }}'}
  {% endif %}
{% endmacro %}
