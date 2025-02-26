with pivoted as (
    select
        *
    from
        {{ ref('int_sm_joined') }}
    pivot(
        max(answer_text) for question_text in (
            'Are you of Hispanic, Latino, or of Spanish origin?' as hispanic_origin,
            'How likely is it that you would recommend this production to a friend or colleague?' as recommend_production,
            'How likely is it that you would recommend Lyric to a friend or colleague?' as recommend_lyric,
            'How would you describe yourself?' as race,
            'Overall, considering your entire experience, how would you rate it? This could include how you were treated, parking, concessions, pre and post-performance events, rest rooms etc.' as experience_rating,
            'Please use the space below to share any additional thoughts you might have that would be helpful to Lyric.' as additional_thoughts,
            'Were there any aspects of your overall experience that were problematic for you or could have been improved? (Please be as specific as possible.)' as negative_comments,
            'What about your overall experience did you find especially positive or appealing? (Please be as specific as possible.)' as positive_comments,
            'Which of the following categories best describes your age?' as age
        )
    )
)

select
    *
from
    pivoted