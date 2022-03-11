{{ config(materialized='table') }}

WITH current_week AS (
    
    SELECT
        week 
    FROM {{ref('date_base_xf')}}
    WHERE day=CURRENT_DATE-7

)

SELECT
    person_id,
    marketing_created_date AS created_date,
    CASE WHEN channel_lead_creation LIKE '$https$' THEN 'organic'
        WHEN channel_lead_creation IS null THEN 'organic'
        ELSE channel_lead_creation 
    END AS channel_lead_creation,
    CASE WHEN medium_lead_creation LIKE '$https$' THEN 'search'
        ELSE medium_lead_creation 
    END AS medium_lead_creation,
    CASE WHEN source_lead_creation LIKE '$https$' THEN 'google'
        ELSE source_lead_creation 
    END AS source_lead_creation,
    country,
    company_size_rev 
FROM {{ref('person_source_xf')}}
LEFT JOIN {{ref('date_base_xf')}} ON
marketing_created_date=date_base_xf.day
LEFT JOIN current_week ON 
date_base_xf.week=current_week.week
WHERE current_week.week IS NOT null
AND marketing_created_date IS NOT null
AND person_owner_id != '00Ga0000003Nugr' -- AO-Fake Leads
AND email NOT LIKE '%act-on.com'
AND lead_source = 'Marketing'