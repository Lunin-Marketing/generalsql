{{ config(materialized='table') }}

WITH last_week AS (

    SELECT
        week 
    FROM {{ref('date_base_xf')}}
    WHERE day=CURRENT_DATE-7

)

SELECT
    person_id,
    mql_most_recent_date,
    channel_lead_creation,
    medium_lead_creation,
    source_lead_creation,
    country,
    company_size_rev
FROM {{ref('person_source_xf')}}
LEFT JOIN {{ref('date_base_xf')}} ON
person_source_xf.mql_most_recent_date=date_base_xf.day
LEFT JOIN last_week ON 
date_base_xf.week=last_week.week
WHERE last_week.week IS NOT null
AND mql_most_recent_date IS NOT null
AND person_owner_id != '00Ga0000003Nugr' -- AO-Fake Leads
AND email NOT LIKE '%act-on.com'
AND lead_source = 'Marketing'