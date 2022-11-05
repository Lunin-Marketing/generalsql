{{ config(materialized='table') }}

WITH won_opp AS (

    SELECT
        won_id,
        won_date,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        target_account,
        industry_bucket
    FROM {{ref('funnel_report_all_time_won')}}

),  closing_opp AS (

    SELECT
        closing_id,
        closing_date,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        industry_bucket,
        target_account,
        channel_bucket
    FROM {{ref('funnel_report_all_time_closing')}}
    
), final AS (

    SELECT
        won_id,
        closing_date,
        won_date,
        won_opp.account_global_region,
        won_opp.company_size_rev,
        won_opp.opp_lead_source,
        won_opp.segment,
        won_opp.industry,
        won_opp.industry_bucket,
        won_opp.target_account,
        won_opp.channel_bucket,
        {{ dbt_utils.datediff("closing_date","won_date",'day')}} AS closing2cw_velocity
    FROM won_opp
    LEFT JOIN closing_opp ON 
    won_opp.won_id=closing_opp.closing_id
)

SELECT
    account_global_region,
    company_size_rev,
    opp_lead_source,
    segment,
    industry,
    channel_bucket,
    target_account,
    industry_bucket,
    won_date,
    closing2cw_velocity
FROM final