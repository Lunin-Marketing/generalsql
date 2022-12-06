{{ config(materialized='table') }}

SELECT DISTINCT
    opportunity_id,
    user_source_xf.profile_name,
    person_source_xf.created_date
FROM {{ref('opp_source_xf')}}
LEFT JOIN {{ref('person_source_xf')}} ON
opp_source_xf.contact_role_contact_id=person_source_xf.person_id
LEFT JOIN {{ref('user_source_xf')}} ON
person_source_xf.created_by_id=user_source_xf.user_id
WHERE opp_source_xf.channel_bucket = 'Unknown'
AND opp_source_xf.discovery_date >= '2022-06-01'
AND opp_source_xf.type = 'New Business'