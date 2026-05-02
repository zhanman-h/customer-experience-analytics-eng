{% macro get_order_amount(unit_price, quantity) %}
    round(({{ unit_price }} * {{ quantity }})::numeric, 2)
{% endmacro %}