{{ config(materialized='table') }}

SELECT
opportunity_id,
opportunity_name,
opp_source_xf.account_name,
user_name AS owner_name,
close_day AS close_date,
opp_lead_source,
opp_channel_opportunity_creation, 
opp_medium_opportunity_creation,
opp_source_opportunity_creation, 
opp_channel_lead_creation,
opp_medium_lead_creation,
opp_source_lead_creation,
opp_offer_asset_name_lead_creation,
opp_offer_asset_subtype_lead_creation,
opp_offer_asset_topic_lead_creation,
opp_offer_asset_type_lead_creation,
type,
opp_source_xf.target_account,
case 
when type in ('New Business') then 'New Business'
when type in ('UpSell','Non-Monetary Mod','Admin Opp','Trigger Up','Trigger Down','Trigger Renewal','Renewal','Multiyear Renewal','Admin Conversion','One Time','Downsell') then 'Upsell'
else null
end as grouped_type,
acv_deal_size_usd AS acv,
billing_country AS country,
account_global_region,
opp_source_xf.company_size_rev,
opp_source_xf.segment,
opp_source_xf.industry,
opp_source_xf.industry_bucket,
channel_bucket_details,
opp_source_xf.channel_bucket
FROM {{ref('opp_source_xf')}}
LEFT JOIN {{ref('user_source_xf')}} ON
opp_source_xf.owner_id=user_source_xf.user_id
LEFT JOIN {{ref('account_source_xf')}} ON
opp_source_xf.account_id=account_source_xf.account_id
WHERE close_day IS NOT null
AND stage_name NOT IN ('Closed - Duplicate','Closed - Admin Removed')
AND is_won = '1'

