{{ config(materialized='table') }}

SELECT
opportunity_id,
opportunity_name,
opp_source_xf.is_closed,
opp_source_xf.is_won,
created_date,
stage_name,
opp_lead_source,
opp_channel_opportunity_creation, 
opp_medium_opportunity_creation,
opp_source_opportunity_creation, 
type,
billing_country AS country
FROM {{ref('opp_source_xf')}}
--FROM "acton".dbt_actonmarketing.opp_source_xf
LEFT JOIN {{ref('account_source_xf')}} ON
--LEFT JOIN "acton".dbt_actonmarketing.account_source_xf ON 
opp_source_xf.account_id=account_source_xf.account_id
WHERE created_date IS NOT null
AND stage_name NOT IN ('Closed - Duplicate','Closed - Admin Removed')

