{{ config(materialized='table') }}

SELECT DISTINCT
    person_id,
    user_source_xf.profile_name,
    created_date
FROM {{ref('person_source_xf')}}
LEFT JOIN {{ref('user_source_xf')}} ON
person_source_xf.created_by_id=user_source_xf.user_id
WHERE channel_bucket = 'Unknown'
AND mql_most_recent_date >= '2022-10-01'