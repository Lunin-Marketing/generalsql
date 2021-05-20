{{ config(materialized='table') }}

SELECT
opportunity_id,
opportunity_name,
full_name AS owner_name,
owner_id,
is_closed,
is_won,
discovery_date,
stage_name,
opp_lead_source,
opp_channel_opportunity_creation, 
opp_medium_opportunity_creation,
opp_source_opportunity_creation, 
type,
acv
FROM "defaultdb".dbt_actonmarketing.opp_source_xf
LEFT JOIN "defaultdb".dbt_actonmarketing.user_source_xf ON
LEFT(opp_source_xf.owner_id,15)=user_source_xf.user_id
WHERE discovery_date IS NOT null
AND stage_name NOT IN ('Closed - Duplicate','Closed - Admin Removed')

