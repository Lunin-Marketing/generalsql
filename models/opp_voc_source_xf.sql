{{ config(materialized='table') }}

SELECT
    opportunity_id,
    opportunity_name,
    user_name AS owner_name,
    opp_source_xf.account_name,
    owner_id,
    opp_source_xf.is_closed,
    opp_source_xf.is_won,
    voc_date AS negotiation_date,
    stage_name,
    opp_lead_source,
    CASE 
        WHEN type in ('New Business') THEN 'New Business'
        WHEN type in ('UpSell','Non-Monetary Mod','Admin Opp','Trigger Up','Trigger Down','Trigger Renewal','Renewal','Multiyear Renewal','Admin Conversion','One Time','Downsell') THEN 'Upsell'
        ELSE null
    END AS grouped_type,
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
    opp_source_xf.company_size_rev,
    opp_source_xf.segment,
    opp_source_xf.industry,
    opp_source_xf.industry_bucket,
    opp_source_xf.channel_bucket,
    opp_source_xf.opp_offer_asset_name_lead_creation
FROM {{ref('opp_source_xf')}}
LEFT JOIN {{ref('user_source_xf')}} ON
opp_source_xf.owner_id=user_source_xf.user_id
LEFT JOIN {{ref('account_source_xf')}} ON
opp_source_xf.account_id=account_source_xf.account_id
WHERE voc_date IS NOT null
AND stage_name NOT IN ('Closed - Duplicate','Closed - Admin Removed','SQL','Discovery','Demo','Implement')
ORDER BY 7 DESC