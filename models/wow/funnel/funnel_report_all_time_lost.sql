{{ config(materialized='table') }}

WITH base AS (

        SELECT DISTINCT
        opp_source_xf.opportunity_id AS lost_id,
        CONCAT('https://acton.my.salesforce.com/',opp_source_xf.opportunity_id) AS lost_url,
        acv_deal_size_usd,
        opp_source_xf.close_date AS lost_date,
        opp_source_xf.account_global_region,
        opp_source_xf.company_size_rev,
        opp_lead_source,
        stage_name,
        opp_source_xf.segment,
        opp_source_xf.industry,
        channel_bucket
    FROM {{ref('opp_source_xf')}}
    LEFT JOIN {{ref('account_source_xf')}} ON
    opp_source_xf.account_id=account_source_xf.account_id
    AND opp_source_xf.close_date IS NOT null
    AND opp_source_xf.type = 'New Business'
    AND opp_source_xf.stage_name IN ('Closed – Lost No Resources / Budget','Closed – Lost Not Ready / No Decision','Closed – Lost Product Deficiency','Closed - Lost to Competitor')

)

SELECT *
FROM base