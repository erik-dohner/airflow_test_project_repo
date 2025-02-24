with responses as (
    select
        respondent_id, 
        question_id, 
        -- convert nan to 'open_ended_response' for joining 
        -- remove trailing '.0' pattern with regex matching, needed for joining, as well
        regexp_replace(replace(choice_id, 'nan', 'open_ended_response'), '\\.0$', '') AS answer_id, 
        regexp_replace(other_id, '\\.0$', '') AS other_id,
        text
    from
        {{ source('raw_data', 'raw_sm_responses') }}
)

select
    *
from 
    responses