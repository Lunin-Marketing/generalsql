{{ config(materialized='table') }}

SELECT
    person_id,
    DATE_TRUNC('month',mql_most_recent_date) AS mql_month,
    lead_score
FROM {{ref('person_source_xf')}}
WHERE lead_score < '120'
AND (LOWER(is_hand_raiser) = 'yes'
AND LOWER(looking_for_ma) = 'yes'
)
AND mql_most_recent_date >= '2022-07-01'