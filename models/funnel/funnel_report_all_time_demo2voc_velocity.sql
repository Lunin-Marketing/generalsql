{{ config(materialized='table') }}

WITH demo_opp AS (

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

),  voc_opp AS (

    SELECT
        voc_id,
        voc_date,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        channel_bucket_details,
        target_account,
        industry_bucket
    FROM {{ref('funnel_report_all_time_voc')}}
    
), final AS (

    SELECT
        voc_id,
        demo_date,
        voc_date,
        voc_opp.account_global_region,
        voc_opp.company_size_rev,
        voc_opp.opp_lead_source,
        voc_opp.segment,
        voc_opp.industry,
        voc_opp.channel_bucket,
        voc_opp.channel_bucket_details,
        voc_opp.target_account,
        voc_opp.industry_bucket,
        {{ datediff("demo_date","voc_date",'day')}} AS demo2voc_velocity
    FROM voc_opp
    LEFT JOIN demo_opp ON 
    voc_opp.voc_id=demo_opp.demo_id
)

SELECT
    account_global_region,
    company_size_rev,
    opp_lead_source,
    segment,
    industry,
    channel_bucket,
    channel_bucket_details,
    target_account,
    industry_bucket,
    voc_date,
    demo2voc_velocity
FROM final