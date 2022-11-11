{{ config(materialized='table') }}

SELECT
    person_id
FROM {{ref('person_source_xf')}}
WHERE lead_score = 50
AND (looking_for_ma IS NOT null
    OR is_hand_raiser = true)