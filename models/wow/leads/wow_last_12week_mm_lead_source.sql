{{ config(materialized='table') }}

WITH last_12_weeks AS (
SELECT DISTINCT
week 
FROM {{ref('date_base_xf')}}
--FROM "acton".dbt_actonmarketing.date_base_xf
WHERE day BETWEEN CURRENT_DATE-84 AND CURRENT_DATE-7

), final AS (
    
SELECT
last_12_weeks.week,
COUNT(lead_id) AS leads
FROM {{ref('lead_source_xf')}}
--FROM "acton".dbt_actonmarketing.lead_source_xf
LEFT JOIN {{ref('date_base_xf')}} ON
--LEFT JOIN "acton".dbt_actonmarketing.date_base_xf ON
lead_source_xf.marketing_created_date=date_base_xf.day
LEFT JOIN last_12_weeks ON 
date_base_xf.week=last_12_weeks.week
WHERE last_12_weeks.week IS NOT null
AND marketing_created_date IS NOT null
AND lead_owner != '00Ga0000003Nugr' -- AO-Fake Leads
AND email NOT LIKE '%act-on.com'
AND lead_source = 'Marketing'
AND company_size_rev IN ('Enterprise','Mid-Market')
GROUP BY 1

)

SELECT
week,
SUM(leads) AS leads
FROM final
GROUP BY 1