{{ config(materialized='table') }}

WITH last_12_weeks AS (

    SELECT DISTINCT
        week 
    FROM {{ref('date_base_xf')}}
    WHERE day BETWEEN CURRENT_DATE-84 AND CURRENT_DATE-7

)

SELECT
    person_id,
    date_base_xf.week AS created_date,
    offer_asset_type_lead_creation,
    person_status
FROM {{ref('person_source_xf')}}
LEFT JOIN {{ref('date_base_xf')}} ON
person_source_xf.marketing_created_date=date_base_xf.day
LEFT JOIN last_12_weeks ON 
date_base_xf.week=last_12_weeks.week
WHERE last_12_weeks.week IS NOT null
AND marketing_created_date IS NOT null
AND person_owner_id != '00Ga0000003Nugr' -- AO-Fake Leads
AND email NOT LIKE '%act-on.com'
AND is_hand_raiser = TRUE
AND LOWER(person_status) NOT IN ('current customer','partner','bad data','no fit')