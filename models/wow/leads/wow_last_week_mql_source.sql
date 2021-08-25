{{ config(materialized='table') }}

WITH last_week AS (
SELECT
week 
FROM "acton".dbt_actonmarketing.date_base_xf
WHERE day=CURRENT_DATE-7
)

SELECT
lead_id,
mql_created_date,
channel_lead_creation,
medium_lead_creation,
source_lead_creation,
country,
company_size_rev
FROM "acton".dbt_actonmarketing.lead_source_xf
LEFT JOIN "acton".dbt_actonmarketing.date_base_xf ON
lead_source_xf.mql_created_date=date_base_xf.day
LEFT JOIN last_week ON 
date_base_xf.week=last_week.week
WHERE last_week.week IS NOT null
AND mql_created_date IS NOT null
AND lead_owner != '00Ga0000003Nugr' -- AO-Fake Leads
AND email NOT LIKE '%act-on.com'
AND lead_source = 'Marketing'
