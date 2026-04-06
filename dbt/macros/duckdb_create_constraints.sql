{% macro duckdb__create_primary_key(
    table_model,
    column_names,
    verify_permissions,
    quote_columns=false,
    constraint_name=none,
    lookup_cache=none
) %}
    {%- set relation = api.Relation.create(
        database=table_model.database,
        schema=table_model.schema,
        identifier=table_model.alias
    ) -%}

    {%- set cols_csv = "'" ~ (column_names | join(',')) ~ "'" -%}
    {%- set exists_sql -%}
        select count(*) as cnt
        from duckdb_constraints()
        where schema_name = '{{ relation.schema }}'
          and table_name = '{{ relation.identifier }}'
          and constraint_type = 'PRIMARY KEY'
          and array_to_string(constraint_column_names, ',') = {{ cols_csv }}
    {%- endset -%}

    {%- set exists_res = run_query(exists_sql) -%}
    {%- set exists = exists_res.columns[0].values()[0] > 0 -%}

    {% if exists %}
        {{ return(true) }}
    {% endif %}

    {%- set cols = [] -%}
    {%- for col in column_names -%}
        {%- do cols.append(adapter.quote(col) if quote_columns else col) -%}
    {%- endfor -%}

    {% do run_query("alter table " ~ relation ~ " add primary key (" ~ cols | join(', ') ~ ")") %}
    {{ return(true) }}
{% endmacro %}


{% macro duckdb__create_unique_key(
    table_model,
    column_names,
    verify_permissions,
    quote_columns=false,
    constraint_name=none,
    lookup_cache=none
) %}
    {# DuckDB cannot do ALTER TABLE ADD CONSTRAINT UNIQUE post-create #}
    {{ log('duckdb__create_unique_key: skipped; DuckDB does not support ALTER TABLE ADD CONSTRAINT UNIQUE', info=true) }}
    {{ return(false) }}
{% endmacro %}


{% macro duckdb__create_foreign_key(
    pk_model,
    pk_column_names,
    fk_model,
    fk_column_names,
    verify_permissions,
    quote_columns=false,
    constraint_name=none,
    lookup_cache=none
) %}
    {# DuckDB cannot do ALTER TABLE ADD CONSTRAINT FOREIGN KEY post-create #}
    {{ log('duckdb__create_foreign_key: skipped; DuckDB does not support ALTER TABLE ADD CONSTRAINT FOREIGN KEY', info=true) }}
    {{ return(false) }}
{% endmacro %}


{% macro duckdb__create_not_null(
    pk_model,
    pk_column_names,
    fk_model,
    fk_column_names,
    verify_permissions,
    quote_columns=false,
    lookup_cache=none
) %}
    {# For not_null tests, dbt_constraints passes the constrained table/cols via fk_* args #}

    {%- set relation = api.Relation.create(
        database=fk_model.database,
        schema=fk_model.schema,
        identifier=fk_model.alias
    ) -%}

    {%- for col in fk_column_names -%}
        {%- set col_sql = adapter.quote(col) if quote_columns else col -%}
        {%- set sql -%}
            alter table {{ relation }} alter column {{ col_sql }} set not null
        {%- endset -%}
        {% do run_query(sql) %}
    {%- endfor -%}

    {{ return(true) }}
{% endmacro %}


{% macro duckdb__unique_constraint_exists(
    table_relation,
    column_names,
    lookup_cache=none
) %}
    {%- set cols_csv = "'" ~ (column_names | join(',')) ~ "'" -%}
    {%- set sql -%}
        select count(*) as cnt
        from duckdb_constraints()
        where schema_name = '{{ table_relation.schema }}'
          and table_name = '{{ table_relation.identifier }}'
          and constraint_type = 'UNIQUE'
          and array_to_string(constraint_column_names, ',') = {{ cols_csv }}
    {%- endset -%}

    {%- set res = run_query(sql) -%}
    {{ return(res.columns[0].values()[0] > 0) }}
{% endmacro %}


{% macro duckdb__foreign_key_exists(
    table_relation,
    column_names,
    lookup_cache=none
) %}
    {%- set cols_csv = "'" ~ (column_names | join(',')) ~ "'" -%}
    {%- set sql -%}
        select count(*) as cnt
        from duckdb_constraints()
        where schema_name = '{{ table_relation.schema }}'
          and table_name = '{{ table_relation.identifier }}'
          and constraint_type = 'FOREIGN KEY'
          and array_to_string(constraint_column_names, ',') = {{ cols_csv }}
    {%- endset -%}

    {%- set res = run_query(sql) -%}
    {{ return(res.columns[0].values()[0] > 0) }}
{% endmacro %}


{% macro duckdb__have_references_priv(
    table_relation,
    verify_permissions,
    lookup_cache=none
) %}
    {# DuckDB does not use warehouse-style REFERENCES privileges in the usual way #}
    {{ return(true) }}
{% endmacro %}


{% macro duckdb__have_ownership_priv(
    table_relation,
    verify_permissions,
    lookup_cache=none
) %}
    {# DuckDB does not use warehouse-style OWNERSHIP privileges in the usual way #}
    {{ return(true) }}
{% endmacro %} 
