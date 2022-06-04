{{ config(materialized='table') }}

WITH base AS (

    SELECT DISTINCT
        global_region,
        company_size_rev,
        lead_source,
        segment,
        industry,
        channel_bucket,
        created_date AS date
    FROM {{ref('funnel_report_all_time_leads')}}
    UNION ALL
    SELECT DISTINCT
        global_region,
        company_size_rev,
        lead_source,
        segment,
        industry,
        channel_bucket,
        mql_date
    FROM {{ref('funnel_report_all_time_mqls')}}
    UNION ALL
    SELECT DISTINCT
        global_region,
        company_size_rev,
        lead_source,
        segment,
        industry,
        channel_bucket,
        sal_date
    FROM {{ref('funnel_report_all_time_sals')}}
    UNION ALL
    SELECT DISTINCT
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        sql_date
    FROM {{ref('funnel_report_all_time_sqls')}}
    UNION ALL
    SELECT DISTINCT
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        sqo_date
    FROM {{ref('funnel_report_all_time_sqos')}}
    UNION ALL
    SELECT DISTINCT
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        demo_date
    FROM {{ref('funnel_report_all_time_demo')}}
    UNION ALL
    SELECT DISTINCT
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        voc_date
    FROM {{ref('funnel_report_all_time_voc')}}
    UNION ALL
    SELECT DISTINCT
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        closing_date
    FROM {{ref('funnel_report_all_time_closing')}}
    UNION ALL
    SELECT DISTINCT
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        won_date
    FROM {{ref('funnel_report_all_time_won')}}
    UNION ALL
    SELECT DISTINCT
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        lost_date
    FROM {{ref('funnel_report_all_time_lost')}}

), final AS (

    SELECT DISTINCT
        base.lead_source
    FROM base
)

SELECT *
FROM final