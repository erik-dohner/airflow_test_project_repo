{{ config(materialized='incremental', unique_key='respondent_id') }}

with fct_responses as (
    select
        *
    from
        {{ ref('int_sm_final') }}

    {% if is_incremental() %}
    where response_date > 
        (select
            max(response_date)
        from
            {{ this }} 
        )
    {% endif %}
)

select
    *
from
    fct_responses