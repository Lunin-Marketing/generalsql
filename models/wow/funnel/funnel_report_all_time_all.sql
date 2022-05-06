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
    
), lead_base AS (

    SELECT
        COUNT(DISTINCT lead_id) AS leads,
        global_region,
        company_size_rev,
        lead_source,
        segment,
        industry,
        channel_bucket,
        created_date
    FROM {{ref('funnel_report_all_time_leads')}}
    GROUP BY 2,3,4,5,6,7,8

), mql_base AS (

    SELECT 
        COUNT(DISTINCT mql_id) AS mqls,
        global_region,
        company_size_rev,
        lead_source,
        segment,
        industry,
        channel_bucket,
        mql_date
    FROM {{ref('funnel_report_all_time_mqls')}}
    GROUP BY 2,3,4,5,6,7,8

), sal_base AS (

    SELECT 
        COUNT(DISTINCT sal_id) AS sals,
        global_region,
        company_size_rev,
        lead_source,
        segment,
        industry,
        channel_bucket,
        sal_date
    FROM {{ref('funnel_report_all_time_sals')}}
    GROUP BY 2,3,4,5,6,7,8

), sql_base AS (

    SELECT 
        COUNT(DISTINCT sql_id) AS sqls,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        sql_date
    FROM {{ref('funnel_report_all_time_sqls')}}
    GROUP BY 2,3,4,5,6,7,8

), sqo_base AS (

    SELECT 
        COUNT(DISTINCT sqo_id) AS sqos,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        sqo_date
    FROM {{ref('funnel_report_all_time_sqos')}}
    GROUP BY 2,3,4,5,6,7,8

), demo_base AS (

    SELECT 
        COUNT(DISTINCT demo_id) AS demos,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        demo_date
    FROM {{ref('funnel_report_all_time_demo')}}
    GROUP BY 2,3,4,5,6,7,8

), voc_base AS (

    SELECT 
        COUNT(DISTINCT voc_id) AS vocs,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        voc_date
    FROM {{ref('funnel_report_all_time_voc')}}
    GROUP BY 2,3,4,5,6,7,8

), closing_base AS (

    SELECT 
        COUNT(DISTINCT closing_id) AS closing,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        closing_date
    FROM {{ref('funnel_report_all_time_closing')}}
    GROUP BY 2,3,4,5,6,7,8

), won_base AS (

    SELECT 
        COUNT(DISTINCT won_id) AS won,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        won_date
    FROM {{ref('funnel_report_all_time_won')}}
    GROUP BY 2,3,4,5,6,7,8

), lost_base AS (

    SELECT 
        COUNT(DISTINCT lost_id) AS lost,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        lost_date
    FROM {{ref('funnel_report_all_time_lost')}}
    GROUP BY 2,3,4,5,6,7,8

)

SELECT 
    base.company_size_rev,
    base.global_region,
    base.lead_source,
    base.segment,
    base.industry,
    base.channel_bucket,
    base.date,
    CASE 
        WHEN SUM(leads) IS null THEN 0
        ELSE SUM(leads) 
    END AS leads,
    CASE 
        WHEN SUM(mqls) IS null THEN 0
        ELSE SUM(mqls) 
    END AS mqls,
    CASE 
        WHEN SUM(sals) IS null THEN 0
        ELSE SUM(sals) 
    END AS sals,
    CASE 
        WHEN SUM(sqls) IS null THEN 0
        ELSE SUM(sqls) 
    END AS sqls,
    CASE 
        WHEN SUM(sqos) IS null THEN 0
        ELSE SUM(sqos) 
    END AS sqos,
    CASE 
        WHEN SUM(demos) IS null THEN 0
        ELSE SUM(demos) 
    END AS demos,
    CASE 
        WHEN SUM(vocs) IS null THEN 0
        ELSE SUM(vocs) 
    END AS vocs,
    CASE 
        WHEN SUM(closing) IS null THEN 0
        ELSE SUM(closing) 
    END AS closing,
    CASE 
        WHEN SUM(won) IS null THEN 0
        ELSE SUM(won) 
    END AS won,
    CASE 
        WHEN SUM(lost) IS null THEN 0
        ELSE SUM(lost) 
    END AS lost
    FROM base
    LEFT JOIN lead_base ON 
    base.company_size_rev=lead_base.company_size_rev
    AND base.global_region=lead_base.global_region
    AND base.lead_source=lead_base.lead_source
    AND base.segment=lead_base.segment
    AND base.industry=lead_base.industry
    AND base.channel_bucket=lead_base.channel_bucket
    AND base.date=lead_base.created_date
    LEFT JOIN mql_base ON
    base.company_size_rev=mql_base.company_size_rev
    AND base.global_region=mql_base.global_region
    AND base.lead_source=mql_base.lead_source
    AND base.segment=mql_base.segment
    AND base.industry=mql_base.industry
    AND base.channel_bucket=mql_base.channel_bucket
    AND base.date=mql_base.mql_date
    LEFT JOIN sal_base ON
    base.company_size_rev=sal_base.company_size_rev
    AND base.global_region=sal_base.global_region
    AND base.lead_source=sal_base.lead_source
    AND base.segment=sal_base.segment
    AND base.industry=sal_base.industry
    AND base.channel_bucket=sal_base.channel_bucket
    AND base.date=sal_base.sal_date
    LEFT JOIN sql_base ON
    base.company_size_rev=sql_base.company_size_rev
    AND base.global_region=sql_base.account_global_region
    AND base.lead_source=sql_base.opp_lead_source
    AND base.segment=sql_base.segment
    AND base.industry=sql_base.industry
    AND base.channel_bucket=sql_base.channel_bucket
    AND base.date=sql_base.sql_date
    LEFT JOIN sqo_base ON
    base.company_size_rev=sqo_base.company_size_rev
    AND base.global_region=sqo_base.account_global_region
    AND base.lead_source=sqo_base.opp_lead_source
    AND base.segment=sqo_base.segment
    AND base.industry=sqo_base.industry
    AND base.channel_bucket=sqo_base.channel_bucket
    AND base.date=sqo_base.sqo_date
    LEFT JOIN demo_base ON
    base.company_size_rev=demo_base.company_size_rev
    AND base.global_region=demo_base.account_global_region
    AND base.lead_source=demo_base.opp_lead_source
    AND base.segment=demo_base.segment
    AND base.industry=demo_base.industry
    AND base.channel_bucket=demo_base.channel_bucket
    AND base.date=demo_base.demo_date
    LEFT JOIN voc_base ON
    base.company_size_rev=voc_base.company_size_rev
    AND base.global_region=voc_base.account_global_region
    AND base.lead_source=voc_base.opp_lead_source
    AND base.segment=voc_base.segment
    AND base.industry=voc_base.industry
    AND base.channel_bucket=voc_base.channel_bucket
    AND base.date=voc_base.voc_date
    LEFT JOIN closing_base ON
    base.company_size_rev=closing_base.company_size_rev
    AND base.global_region=closing_base.account_global_region
    AND base.lead_source=closing_base.opp_lead_source
    AND base.segment=closing_base.segment
    AND base.industry=closing_base.industry
    AND base.channel_bucket=closing_base.channel_bucket
    AND base.date=closing_base.closing_date
    LEFT JOIN won_base ON
    base.company_size_rev=won_base.company_size_rev
    AND base.global_region=won_base.account_global_region
    AND base.lead_source=won_base.opp_lead_source
    AND base.segment=won_base.segment
    AND base.industry=won_base.industry
    AND base.channel_bucket=won_base.channel_bucket
    AND base.date=won_base.won_date
    LEFT JOIN lost_base ON
    base.company_size_rev=lost_base.company_size_rev
    AND base.global_region=lost_base.account_global_region
    AND base.lead_source=lost_base.opp_lead_source
    AND base.segment=lost_base.segment
    AND base.industry=lost_base.industry
    AND base.channel_bucket=lost_base.channel_bucket
    AND base.date=lost_base.lost_date
    GROUP BY 1,2,3,4,5,6,7