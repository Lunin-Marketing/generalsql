{{ config(materialized='table') }}

SELECT
    lead_id,
    email,
    lead_source,
    is_converted,
    is_hand_raiser,
    mql_created_date,
    mql_most_recent_date,
    lead_status,
    country,
    lead_owner,
    global_region
FROM {{ref('lead_source_ss_xf')}}
WHERE mql_most_recent_date IS NOT null
AND lead_owner != '00Ga0000003Nugr' -- AO-Fake Leads
AND email NOT LIKE '%act-on.com'
AND lead_source = 'Marketing'
