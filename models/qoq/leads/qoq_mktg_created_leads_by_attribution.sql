{{ config(materialized='table') }}

WITH last_quarter AS (

    SELECT DISTINCT
        quarter 
    FROM {{ref('date_base_xf')}}
    WHERE day BETWEEN CURRENT_DATE-90 AND CURRENT_DATE

)

SELECT
    person_id,
    last_quarter.quarter,
    channel_lead_creation,
    medium_lead_creation,
    source_lead_creation,
    country,
    company_size_rev 
FROM {{ref('person_source_xf')}}
LEFT JOIN {{ref('date_base_xf')}} ON
marketing_created_date=date_base_xf.day
LEFT JOIN last_quarter ON 
date_base_xf.quarter=last_quarter.quarter
WHERE last_quarter.quarter IS NOT null
AND marketing_created_date IS NOT null
AND person_owner_id != '00Ga0000003Nugr' -- AO-Fake Leads
AND email NOT LIKE '%act-on.com'
AND lead_source = 'Marketing'