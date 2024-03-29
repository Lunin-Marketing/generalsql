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

),  demo_opp AS (

    SELECT
        demo_id,
        demo_date,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        channel_bucket_details,
        target_account,
        industry_bucket
    FROM {{ref('funnel_report_all_time_demo')}}
    
), final AS (

    SELECT
        demo_id,
        demo_date,
        sqo_date,
        demo_opp.account_global_region,
        demo_opp.company_size_rev,
        demo_opp.opp_lead_source,
        demo_opp.segment,
        demo_opp.industry,
        demo_opp.channel_bucket,
        demo_opp.channel_bucket_details,
        demo_opp.target_account,
        demo_opp.industry_bucket,
        {{ datediff("sqo_date","demo_date",'day')}} AS sqo2demo_velocity
    FROM demo_opp
    LEFT JOIN sqo_opp ON 
    demo_opp.demo_id=sqo_opp.sqo_id
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
    demo_date,
    sqo2demo_velocity
FROM final