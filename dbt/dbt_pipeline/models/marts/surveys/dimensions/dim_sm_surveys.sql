with dims as (
    select
        survey_id,
        survey_title
    from 
        {{ ref('survey_details') }} as new_data
    where not exists 
        (select
            *
        from
            {{ this }} as old_data
        where 
            new_data.survey_id = old_data.survey_id) 
)

select
    *
from
    dims