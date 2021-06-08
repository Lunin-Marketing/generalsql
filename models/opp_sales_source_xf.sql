{{ config(materialized='table') }}

SELECT
opportunity_id,
opportunity_name,
full_name AS owner_name,
close_date,
opp_lead_source,
opp_channel_opportunity_creation, 
opp_medium_opportunity_creation,
opp_source_opportunity_creation, 
opp_channel_lead_creation,
opp_medium_lead_creation,
opp_source_lead_creation,
type,
case 
when type in ('New Business') then 'New Business'
when type in ('UpSell','Non-Monetary Mod','Admin Opp','Trigger Up','Trigger Down','Trigger Renewal','Renewal','Multiyear Renewal','Admin Conversion','One Time','Downsell') then 'Upsell'
else null
end as grouped_type,
acv
FROM "acton".dbt_actonmarketing.opp_source_xf
LEFT JOIN "acton".dbt_actonmarketing.user_source_xf ON
LEFT(opp_source_xf.owner_id,15)=user_source_xf.user_id
WHERE close_date IS NOT null
--AND stage_name NOT IN ('Closed - Duplicate','Closed - Admin Removed')
AND is_won = '1'

