with other as (
    select distinct
        other_id,
        other_text
    from 
        {{ source('raw_data', 'raw_sm_questions') }}
    where
        other_id is not null
)

select
    *
from
    other