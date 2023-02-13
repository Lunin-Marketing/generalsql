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
        industry_bucket,
        target_account,
        channel_bucket_details,
        channel_bucket
    FROM {{ref('funnel_report_all_time_won')}}

),  sql_opp AS (

    SELECT
        sql_id,
        sql_date,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        industry_bucket,
        target_account,
        channel_bucket_details,
        channel_bucket
    FROM {{ref('funnel_report_all_time_sqls')}}
    
), final AS (

    SELECT
        won_id,
        sql_date,
        won_date,
        won_opp.account_global_region,
        won_opp.company_size_rev,
        won_opp.opp_lead_source,
        won_opp.segment,
        won_opp.industry,
        won_opp.target_account,
        won_opp.industry_bucket,
        won_opp.channel_bucket,
        won_opp.channel_bucket_details,
        {{ datediff("sql_date","won_date",'day')}} AS cw_velocity
    FROM won_opp
    LEFT JOIN sql_opp ON 
    won_opp.won_id=sql_opp.sql_id
)

SELECT
    account_global_region,
    company_size_rev,
    opp_lead_source,
    segment,
    industry,
    channel_bucket,
    channel_bucket_details,
    industry_bucket,
    target_account,
    won_date,
    cw_velocity
FROM final