{{ config(materialized='table') }}

SELECT
opportunity_id,
opportunity_name,
is_closed,
is_won,
created_date,
stage_name,
opp_lead_source,
opp_channel_opportunity_creation, 
opp_medium_opportunity_creation,
opp_source_opportunity_creation, 
type
FROM "defaultdb".dbt_actonmarketing.opp_source_xf
WHERE created_date IS NOT null
AND stage_name NOT IN ('Closed - Duplicate','Closed - Admin Removed')

