-- test to ensure emails are valid 
-- this test will find any emails that DO NOT MATCH the desired pattern

-- need to create macro, this enables jinja templating to make the test dynamic
{% test valid_email(model, column_name) %}

with invalid_emails as (
    select
        *
    from 
        {{ model }}
    where
        NOT REGEXP_CONTAINS({{ column_name }}, r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
)

select * from invalid_emails

{% endtest %}