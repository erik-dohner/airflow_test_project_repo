with cleaned as (
    select
        cast(respondent_id as numeric) as respondent_id, 
        cast(collector_id as numeric) as collector_id,
        date as response_date, 
        email, 
        safe_cast(const_id as numeric) as const_it, -- safe_cast here to convert any strings that cannot be conveted to int, to null instead
        performance_code, 
        production_name,
        survey_id,
        cast(regexp_extract(recommend_lyric, '(\\d+)')as numeric) as recommend_lyric, -- selecting the integer from the string
        cast(regexp_extract(recommend_production, '(\\d+)') as numeric) as recommend_production, -- selecting the integer from the string
        hispanic_origin, 
        race,
        lower(trim(experience_rating, ' ')) as experience_rating,
        additional_thoughts,
        negative_comments,
        positive_comments,
        comments,
        age
    from
        {{ ref('int_sm_full') }}
    where
        const_id is not null -- remove any const_id that couldn't be cast to an int type. This means their const_id wasn't captured during survey submission.
)

select  
    *
from
    cleaned