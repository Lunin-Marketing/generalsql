{{ config(materialized='table') }}

SELECT
    person_id,
    mql_most_recent_date,
    looking_for_ma,
    is_hand_raiser
FROM {{ref('person_source_xf')}}
WHERE lead_score = 50
AND (LOWER(looking_for_ma) = 'yes'
    OR LOWER(is_hand_raiser) = 'true')
AND mql_most_recent_date IS NOT null