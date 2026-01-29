{% test percentage_value(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} <= 0 OR {{ column_name }} >= 100
{% endtest %}