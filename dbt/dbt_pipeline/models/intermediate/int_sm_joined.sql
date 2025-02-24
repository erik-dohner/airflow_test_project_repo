with merge_all as (
    select
        respondent_id,
        r.question_id,
        question_text,
        answer_text,
        other_text,
        text
    from
        {{ ref('stg_sm_responses') }} as r
    left join 
        {{ ref('stg_sm_questions') }} as q
    on 
        r.question_id = q.question_id
    left join
        {{ ref('stg_sm_answers') }} as a
    on 
        r.answer_id = a.answer_id
    left join
        {{ ref('stg_sm_other') }} as o
    on
        r.other_id = o.other_id

),

condensed as (
    select
        respondent_id,
        question_text, 
        replace(answer_text, 'open_ended_response', text) as answer_text
    from
        merge_all
)


select
    *
from
    condensed