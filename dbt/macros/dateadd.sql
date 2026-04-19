{% macro duckdb__dateadd(datepart, interval, from_date_or_timestamp) %}
    {{ from_date_or_timestamp }} + INTERVAL ({{ interval }}) {{ datepart }}
{% endmacro %}

{% macro databricks__dateadd(datepart, interval, from_date_or_timestamp) %}
    timestampadd({{ datepart }}, {{ interval }}, {{ from_date_or_timestamp }})
{% endmacro %}
