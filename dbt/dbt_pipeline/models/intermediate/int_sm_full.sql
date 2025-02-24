with comments_combined  as (
  select
    respondent_id, 
    cast(
      lower(
        coalesce(positive_comments, '') || '' ||
        coalesce(negative_comments, '') || '' ||
        coalesce(additional_thoughts, '') 
      ) as string
     ) as comments
  from
    {{ ref('int_sm_pivoted') }}
),


prep as (
    select
        p.*, 
        collector_id,
        date, 
        const_id,
        email, 
        performance_code,
        production_name,
        survey_id,
        comments
    from
        {{ ref('int_sm_pivoted') }} as p
    left join
        (select distinct
            respondent_id,
            collector_id,
            date, 
            const_id,
            email, 
            performance_code,
            production_name, 
            survey_id
        from
            {{ source('raw_data', 'raw_sm_responses') }}
        ) as r
    on
        p.respondent_id = r.respondent_id
    left join
        comments_combined as c
    on
        p.respondent_id = c.respondent_id
)

select
    *
from 
    prep
