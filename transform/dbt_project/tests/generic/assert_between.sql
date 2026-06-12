{% test assert_between(model, column_name, min_value, max_value) %}
-- Generic test: fails for any NON-NULL value of column_name outside the inclusive
-- range [min_value, max_value]. Returns the offending rows (dbt fails if any exist).
select
    {{ column_name }} as offending_value
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }})
{% endtest %}
