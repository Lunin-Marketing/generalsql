{{ config(materialized='table') }}

SELECT
    person_id,
    email,
    is_hand_raiser,
    mql_created_date,
    mql_most_recent_date,
    person_status,
    country,
    person_owner_id,
    global_region,
    company_size_rev,
    lead_source,
    segment,
    industry,
    channel_bucket
FROM {{ref('person_source_xf')}}
WHERE mql_most_recent_date IS NOT null
AND person_owner_id != '00Ga0000003Nugr' -- AO-Fake Leads
AND email NOT LIKE '%act-on.com'
--AND lead_source = 'Marketing'