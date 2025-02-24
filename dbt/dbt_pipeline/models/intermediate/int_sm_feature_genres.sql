with genres_matched as (
    select
        f.respondent_id, 
        g.genre
    from  
        {{ ref('int_sm_full_cleaned') }} as f
    left join 
        {{ ref('genres') }} as g
    on 
        f.production_name = g.production_name
)

select
    *
from
    genres_matched