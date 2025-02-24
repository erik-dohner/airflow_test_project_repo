with answers as (
    select distinct
        coalesce(answer_id, 'open_ended_response') as answer_id,
        coalesce(answer_text, 'open_ended_response') as answer_text
    from 
        {{ source('raw_data', 'raw_sm_questions') }}
)

select
    *
from
    answers