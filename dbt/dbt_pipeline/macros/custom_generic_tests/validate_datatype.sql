-- test to confirm a specific column matches a specific datatype  


{% test validate_datatype(model, column_name, expected_type) %}

select
    {{ column_name }}
from 
    {{ model }}
where
    SAFE_CAST({{ column_name }} AS {{ expected_type }}) IS NULL
    AND {{ column_name }} IS NOT NULL

{% endtest %}