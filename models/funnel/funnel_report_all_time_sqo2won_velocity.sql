{{ config(materialized='table') }}

WITH sqo_opp AS (

    SELECT
        sqo_id,
        sqo_date,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        target_account,
        channel_bucket_details,
        channel_bucket,
        industry_bucket
    FROM {{ref('funnel_report_all_time_sqos')}}

), won_opp AS (

    SELECT
        won_id,
        won_date,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        target_account,
        channel_bucket_details,
        channel_bucket,
        industry_bucket
    FROM {{ref('funnel_report_all_time_won')}}
    
), final AS (

    SELECT
        won_id,
        won_date,
        sqo_date,
        won_opp.account_global_region,
        won_opp.company_size_rev,
        won_opp.opp_lead_source,
        won_opp.segment,
        won_opp.industry,
        won_opp.target_account,
        won_opp.channel_bucket,
        won_opp.channel_bucket_details,
        won_opp.industry_bucket,
        {{ datediff("sqo_date","won_date",'day')}} AS sqo2won_velocity
    FROM won_opp
    LEFT JOIN sqo_opp ON 
    won_opp.won_id=sqo_opp.sqo_id
)

SELECT
    account_global_region,
    company_size_rev,
    opp_lead_source,
    segment,
    industry,
    channel_bucket,
    industry_bucket,
    channel_bucket_details,
    target_account,
    won_date,
    sqo2won_velocity
FROM final