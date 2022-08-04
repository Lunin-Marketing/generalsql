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
        won_opp.channel_bucket,
        {{ dbt_utils.datediff("sql_date","won_date",'day')}} AS cw_velocity
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
    won_date,
    AVG(cw_velocity)
FROM final
GROUP BY 1,2,3,4,5,6,7