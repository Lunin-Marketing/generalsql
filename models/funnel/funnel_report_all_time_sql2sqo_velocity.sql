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
        channel_bucket,
        channel_bucket_details,
        target_account,
        industry_bucket
    FROM {{ref('funnel_report_all_time_sqos')}}

),  sql_opp AS (

    SELECT
        sql_id,
        sql_date,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        channel_bucket_details,
        target_account,
        industry_bucket
    FROM {{ref('funnel_report_all_time_sqls')}}
    
), final AS (

    SELECT
        sqo_id,
        sql_date,
        sqo_date,
        sqo_opp.account_global_region,
        sqo_opp.company_size_rev,
        sqo_opp.opp_lead_source,
        sqo_opp.segment,
        sqo_opp.industry,
        sqo_opp.channel_bucket,
        sqo_opp.channel_bucket_details,
        sqo_opp.target_account,
        sqo_opp.industry_bucket,
        {{ datediff("sql_date","sqo_date",'day')}} AS sql2sqo_velocity
    FROM sqo_opp
    LEFT JOIN sql_opp ON 
    sqo_opp.sqo_id=sql_opp.sql_id
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
    sqo_date,
    sql2sqo_velocity
FROM final