{% macro get_time_diff(start_timestamp, end_timestamp, unit='minute') %}

    {% if unit == 'minute' %}
        round(extract(epoch from ({{ end_timestamp }} - {{ start_timestamp }})) / 60.0, 2)
    {% elif unit == 'hour' %}
        round(extract(epoch from ({{ end_timestamp }} - {{ start_timestamp }})) / 3600.0, 2)
    {% else %}
        extract(epoch from ({{ end_timestamp }} - {{ start_timestamp }}))
    {% endif %}

{% endmacro %}