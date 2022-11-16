{{ config(materialized='table') }}

SELECT
    person_id,
    mql_most_recent_date,
    lead_score
FROM {{ref('person_source_xf')}}
WHERE lead_score < '70'
AND (LOWER(is_hand_raiser) = 'yes'
OR LOWER(looking_for_ma) = 'yes'
)
AND mql_most_recent_date >= '2022-09-01'