{{ config(materialized='table') }}

SELECT
    opportunity_id AS sql_id,
    opportunity_name,
    opp_source_xf.is_closed,
    opp_source_xf.is_won,
    created_date,
    stage_name,
    opp_lead_source,
    opp_channel_opportunity_creation, 
    opp_medium_opportunity_creation,
    opp_source_opportunity_creation, 
    opp_channel_lead_creation,
    opp_medium_lead_creation,
    opp_source_lead_creation,
    type,
    billing_country AS country,
    opp_source_xf.company_size_rev,
    account_global_region,
    opp_source_xf.segment,
    opp_source_xf.industry,
    opp_source_xf.industry_bucket,
    channel_bucket
FROM {{ref('opp_source_xf')}}
LEFT JOIN {{ref('account_source_xf')}} ON
opp_source_xf.account_id=account_source_xf.account_id
WHERE created_date IS NOT null
AND stage_name NOT IN ('Closed - Duplicate','Closed - Admin Removed')
--AND stage_name = 'SQL'