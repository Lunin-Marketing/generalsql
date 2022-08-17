

  create  table "acton"."dbt_actonmarketing"."funnel_report_current_quarter_by_week_ss_xf__dbt_tmp"
  as (
    

WITH base_prep AS (

    SELECT DISTINCT
        week,
        global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_quarter_leads_ss"
    UNION ALL 
    SELECT DISTINCT
        week,
        global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_quarter_mqls_ss"
    UNION ALL 
    SELECT DISTINCT
        week,
        global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_quarter_sals_ss"
    UNION ALL 
    SELECT DISTINCT
        week,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_quarter_sqls_ss"

), base AS (

    SELECT DISTINCT
    week,
    global_region
    FROM base_prep

), lead_base AS (

    SELECT
        COUNT(DISTINCT lead_id) AS leads,
        week,
        global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_quarter_leads_ss"
    GROUP BY 2,3

), mql_base AS (
    
    SELECT
        COUNT(DISTINCT mql_id) AS mqls,
        week,
        global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_quarter_mqls_ss"
    GROUP BY 2,3

), sal_base AS (

    SELECT
        COUNT(DISTINCT sal_id) AS sals,
        week,
        global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_quarter_sals_ss"
    GROUP BY 2,3

), sql_base AS (

    SELECT
        COUNT(DISTINCT sql_id) AS sqls,
        week,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_quarter_sqls_ss"
    GROUP BY 2,3
   
)

SELECT
    base.week,
    base.global_region,
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
    END AS sqls
FROM base
LEFT JOIN lead_base ON
base.week=lead_base.week
AND base.global_region=lead_base.global_region
LEFT JOIN mql_base ON
base.week=mql_base.week
AND base.global_region=mql_base.global_region
LEFT JOIN sal_base ON
base.week=sal_base.week
AND base.global_region=sal_base.global_region
LEFT JOIN sql_base ON
base.week=sql_base.week
AND base.global_region=sql_base.account_global_region
GROUP BY 1,2
ORDER BY 1,2
  );