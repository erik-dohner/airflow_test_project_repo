with questions as (
    select distinct
        question_id,
        question_text
    from 
        {{ source('raw_data', 'raw_sm_questions') }}
)

select
    *
from
    questions