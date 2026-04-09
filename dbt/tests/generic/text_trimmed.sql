{% test text_trimmed(model, column_name) %}

SELECT {{ column_name }}
FROM {{ model }}
WHERE TRIM({{ column_name }}) <> {{ column_name }}

{% endtest %}
