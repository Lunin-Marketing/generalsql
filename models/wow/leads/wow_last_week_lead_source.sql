{{ config(materialized='table') }}

WITH current_week AS (
SELECT
week 
FROM {{ref('date_base_xf')}}
--FROM "acton".dbt_actonmarketing.date_base_xf
WHERE day=CURRENT_DATE-7

)

SELECT
lead_id,
marketing_created_date AS created_date,
--COALESCE(converted_date,created_date) AS marketing_date,
channel_lead_creation,
medium_lead_creation,
source_lead_creation,
country,
company_size_rev 
FROM {{ref('lead_source_xf')}}
--FROM "acton".dbt_actonmarketing.lead_source_xf
LEFT JOIN {{ref('date_base_xf')}} ON
--LEFT JOIN "acton".dbt_actonmarketing.date_base_xf ON
marketing_created_date=date_base_xf.day
LEFT JOIN current_week ON 
date_base_xf.week=current_week.week
WHERE current_week.week IS NOT null
AND marketing_created_date IS NOT null
AND lead_owner != '00Ga0000003Nugr' -- AO-Fake Leads
AND email NOT LIKE '%act-on.com'
AND lead_source = 'Marketing'
