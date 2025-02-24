with classificaiton as (
    select
        respondent_id, 
        case when regexp_contains(comments, '\\b(?:parking|train|valet|traffic|garage|cab|search|entrance|leave|road|building|doors|drop|bus|exit|pick)\\b') then 1 else 0 end as arrival_departure,
        case when regexp_contains(comments, '\\b(?:parking|garage|poetry|park|lot)\\b') then 1 else 0 end as arrival_departure_parking,
        case when regexp_contains(comments, '\\b(?:cab|taxi)\\b') then 1 else 0 end as arrival_departure_taxi,
        case when regexp_contains(comments, '\\b(?:time|start|earlier|late)\\b') then 1 else 0 end as arrival_departure_time,
        case when regexp_contains(comments, '\\b(?:train|catch|metra|cta|schedule)\\b') then 1 else 0 end as arrival_departure_train,
        case when regexp_contains(comments, '\\b(?:valet)\\b') then 1 else 0 end as arrival_departure_valet_parking,
        case when regexp_contains(comments, '\\b(?:handicapped|disabled|accessible|hearing|handrail)\\b') then 1 else 0 end as accessibility,
        case when regexp_contains(comments, '\\b(?:bag|security|purse|search)\\b') then 1 else 0 end as bag_check,
        case when regexp_contains(comments, '\\b(?:restroom|rest room|bath room|bathroom|wash room|washroom|toilet)\\b') then 1 else 0 end as bathrooms,
        case when regexp_contains(comments, '\\b(?:words_go_here)\\b') then 1 else 0 end as coat_check,  -- replace words_go_here
        case when regexp_contains(comments, '\\b(?:concession|wine|coffee|bar|drink|cup|espresso|snack|tea|refreshment|champagne|meals|bartender)\\b') then 1 else 0 end as concessions,
        case when regexp_contains(comments, '\\b(?:elevator)\\b') then 1 else 0 end as elevators,
        case when regexp_contains(comments, '\\b(?:intermission)\\b') then 1 else 0 end as intermission,
        case when regexp_contains(comments, '\\b(?:people|audience|patrons|person|crowd|guy|man|woman)\\b') then 1 else 0 end as other_people,
        case when regexp_contains(comments, '\\b(?:phone|cell)\\b') then 1 else 0 end as other_people_cell_tech,
        case when regexp_contains(comments, '\\b(?:child|school)\\b') then 1 else 0 end as other_people_children,
        case when regexp_contains(comments, '\\b(?:cough|drops)\\b') then 1 else 0 end as other_people_coughing,
        case when regexp_contains(comments, '\\b(?:late)\\b') then 1 else 0 end as other_people_late_seating,
        case when regexp_contains(comments, '\\b(?:lean|forward)\\b') then 1 else 0 end as other_people_lean_fwd,
        case when regexp_contains(comments, '\\b(?:rude|behavior)\\b') then 1 else 0 end as other_people_rude,
        case when regexp_contains(comments, '\\b(?:view|sightline|head )\\b') then 1 else 0 end as other_people_sightlines,
        case when regexp_contains(comments, '\\b(?:talk)\\b') then 1 else 0 end as other_people_talking
    from 
        {{ ref('int_sm_full_cleaned') }}
)

select
    *
from
    classificaiton
