{{ config(materialized='table') }}

WITH last_12_weeks AS (
SELECT DISTINCT
week 
FROM "acton".dbt_actonmarketing.date_base_xf
WHERE day BETWEEN CURRENT_DATE-84 AND CURRENT_DATE-7

), final AS (
    
SELECT
last_12_weeks.week,
COUNT(lead_id) AS leads--,
--created_date,
--channel_lead_creation,
--medium_lead_creation,
--source_lead_creation--,
--country,
--company_size_rev
FROM "acton".dbt_actonmarketing.lead_source_xf
LEFT JOIN "acton".dbt_actonmarketing.date_base_xf ON
lead_source_xf.mql_created_date=date_base_xf.day
LEFT JOIN last_12_weeks ON 
date_base_xf.week=last_12_weeks.week
WHERE last_12_weeks.week IS NOT null
AND mql_created_date IS NOT null
AND lead_owner != '00Ga0000003Nugr' -- AO-Fake Leads
AND email NOT LIKE '%act-on.com'
AND lead_source = 'Marketing'
AND company_size_rev = 'Mid-Market'
GROUP BY 1
/*UNION ALL 
SELECT
last_12_weeks.week,
COUNT(contact_id) AS leads--,
--created_date,
--channel_lead_creation,
--medium_lead_creation,
--source_lead_creation--,
--country,
--company_size_rev
FROM "acton".dbt_actonmarketing.contact_source_xf
LEFT JOIN "acton".dbt_actonmarketing.date_base_xf ON
contact_source_xf.created_date=date_base_xf.day
LEFT JOIN last_12_weeks ON 
date_base_xf.week=last_12_weeks.week
WHERE last_12_weeks.week IS NOT null
AND created_date IS NOT null
AND account_owner != '00Ga0000003Nugr'
AND email NOT LIKE '%act-on.com'
AND lead_source = 'Marketing'
AND company_size_rev = 'Mid-Market'
GROUP BY 1*/

)
SELECT
week,
SUM(leads) AS leads
FROM final
GROUP BY 1