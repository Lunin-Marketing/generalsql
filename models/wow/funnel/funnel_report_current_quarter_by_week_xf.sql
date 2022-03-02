{{ config(materialized='table') }}

WITH lead_base AS (

    SELECT
        COUNT(DISTINCT lead_id) AS leads,
        week,
        global_region
    FROM {{ref('funnel_report_current_quarter_leads')}}
    GROUP BY 2,3

), mql_base AS (
    
    SELECT
        COUNT(DISTINCT mql_id) AS mqls,
        week,
        global_region
    FROM {{ref('funnel_report_current_quarter_mqls')}}
    GROUP BY 2,3

), sal_base AS (

    SELECT
        COUNT(DISTINCT sal_id) AS sals,
        week,
        global_region
    FROM {{ref('funnel_report_current_quarter_sals')}}
    GROUP BY 2,3

), sql_base AS (

    SELECT
        COUNT(DISTINCT sql_id) AS sqls,
        week,
        account_global_region
    FROM {{ref('funnel_report_current_quarter_sqls')}}
    GROUP BY 2,3
   
)

SELECT
    week,
    region,
    SUM(leads) AS leads,
    SUM(mqls) AS mqls,
    SUM(sals) AS sals,
    SUM(sqls) AS sqls
FROM lead_base
LEFT JOIN mql_base ON
lead_base.week=mql_base.week
AND lead_base.global_region=mql_base.global_region
LEFT JOIN sal_base ON
lead_base.week=sal_base.week
AND lead_base.global_region=sal_base.global_region
LEFT JOIN sql_base ON
lead_base.week=sql_base.week
AND lead_base.global_region=sql_base.account_global_region
GROUP BY 1,2