with nps as (
    select
        respondent_id, 
        case when recommend_production <=6 then 1 else 0 end as nps_production_detractor,
        case when recommend_production >= 9 then 1 else 0 end as nps_production_prommoter,
        case when recommend_lyric <=6 then 1 else 0 end as nps_lyric_detractor,
        case when recommend_lyric >= 9 then 1 else 0 end as nps_lyric_prommoter,
    from
        {{ ref('int_sm_full_cleaned') }} 
)

select  
    *
from 
    nps