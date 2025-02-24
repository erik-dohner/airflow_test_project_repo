with joins as (
    select
        f.respondent_id,
        collector_id,
        survey_id,
        response_date,
        email,
        const_it,
        performance_code,
        production_name,
        recommend_lyric,
        recommend_production,
        hispanic_origin,
        race,
        experience_rating,
        additional_thoughts,
        negative_comments,
        positive_comments,
        comments,
        age,
        nps_production_detractor,
        nps_production_prommoter,
        nps_lyric_detractor,
        nps_lyric_prommoter,
        arrival_departure,
        arrival_departure_parking,
        arrival_departure_taxi,
        arrival_departure_time,
        arrival_departure_train,
        arrival_departure_valet_parking,
        accessibility,
        bag_check,
        bathrooms,
        coat_check,
        concessions,
        elevators,
        intermission,
        other_people,
        other_people_cell_tech,
        other_people_children,
        other_people_coughing,
        other_people_late_seating,
        other_people_lean_fwd,
        other_people_rude,
        other_people_sightlines,
        other_people_talking,
        genre
    from
        {{ ref('int_sm_full_cleaned') }} as f
    left join
        {{ ref('int_sm_feature_nps') }} as nps
    on
        f.respondent_id = nps.respondent_id
    left join
        {{ ref('int_sm_feature_binary_classification') }} as bc
    on
        f.respondent_id = bc.respondent_id
    left join
        {{ ref('int_sm_feature_genres') }} as g
    on
        f.respondent_id = g.respondent_id
)

select 
    *
from 
    joins