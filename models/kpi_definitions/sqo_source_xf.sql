{{ config(materialized='table') }}

SELECT
opportunity_id,
opportunity_name,
user_name AS owner_name,
opp_source_xf.account_name,
sdr_name,
owner_id,
opp_source_xf.is_closed,
opp_source_xf.is_won,
account_source_xf.is_current_customer,
discovery_date,
created_date,
close_day AS close_date,
stage_name,
opp_lead_source,
primary_campaign_name,
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
opp_source_xf.target_account,
acv_deal_size_usd AS acv,
billing_country AS country,
account_global_region,
opp_source_xf.segment,
opp_source_xf.industry,
opp_source_xf.industry_bucket,
channel_bucket,
channel_bucket_details,
opp_source_xf.company_size_rev,
opp_source_xf.opp_offer_asset_name_lead_creation
FROM {{ref('opp_source_xf')}}
LEFT JOIN {{ref('user_source_xf')}} ON
opp_source_xf.owner_id=user_source_xf.user_id
LEFT JOIN {{ref('account_source_xf')}} ON
opp_source_xf.account_id=account_source_xf.account_id
WHERE discovery_date IS NOT null
AND stage_name NOT IN ('Closed - Duplicate','Closed - Admin Removed','SQL','SQL - No Opportunity')
