{{ config(materialized='table') }}

WITH last_week AS (
SELECT
week 
FROM "acton".dbt_actonmarketing.date_base_xf
WHERE day=CURRENT_DATE-7
)

SELECT
lead_id,
created_date,
offer_asset_type_lead_creation,
lead_status,
is_converted
FROM "acton".dbt_actonmarketing.lead_source_xf
LEFT JOIN "acton".dbt_actonmarketing.date_base_xf ON
lead_source_xf.created_date=date_base_xf.day
LEFT JOIN last_week ON 
date_base_xf.week=last_week.week
WHERE last_week.week IS NOT null
AND created_date IS NOT null
AND lead_owner != '00Ga0000003Nugr' -- AO-Fake Leads
AND email NOT LIKE '%act-on.com'
AND is_hand_raiser = 'true'
