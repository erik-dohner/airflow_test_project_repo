with poor_fair as (
    select
        respondent_id,
        experience_rating,
        email
    from
        {{ ref('fct_sm_responses') }}
    where
        experience_rating in ('poor', 'fair')
)

select
    *
from 
    poor_fair