{{ config(materialized='table') }}

SELECT
opportunity_id,
opportunity_name,
user_name AS owner_name,
owner_id,
opp_source_xf.is_closed,
opp_source_xf.is_won,
discovery_date,
stage_name,
opp_lead_source,
case 
when type in ('New Business') then 'New Business'
when type in ('UpSell','Non-Monetary Mod','Admin Opp','Trigger Up','Trigger Down','Trigger Renewal','Renewal','Multiyear Renewal','Admin Conversion','One Time','Downsell') then 'Upsell'
else null
end as grouped_type,
opp_channel_opportunity_creation, 
opp_channel_lead_creation,
opp_medium_opportunity_creation,
opp_medium_lead_creation,
opp_source_opportunity_creation, 
opp_source_lead_creation,
type,
acv_deal_size_usd AS acv,
billing_country AS country,
account_global_region,
opp_source_xf.segment,
opp_source_xf.industry,
channel_bucket,
opp_source_xf.company_size_rev
FROM {{ref('opp_source_xf')}}
LEFT JOIN {{ref('user_source_xf')}} ON
opp_source_xf.owner_id=user_source_xf.user_id
LEFT JOIN {{ref('account_source_xf')}} ON
opp_source_xf.account_id=account_source_xf.account_id
WHERE discovery_date IS NOT null
AND stage_name NOT IN ('Closed - Duplicate','Closed - Admin Removed','SQL','SQL - No Opportunity')
--AND type = 'New Business'
