

WITH base_prep AS (

    SELECT DISTINCT
        week,
        global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_rolling3mo_leads"
    UNION ALL 
    SELECT DISTINCT
        week,
        global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_rolling3mo_mqls"
    UNION ALL 
    SELECT DISTINCT
        week,
        global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_rolling3mo_sals"
    UNION ALL 
    SELECT DISTINCT
        week,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_rolling3mo_sqls"
    UNION ALL 
    SELECT DISTINCT
        week,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_rolling3mo_sqos"
    UNION ALL 
    SELECT DISTINCT
        week,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_rolling3mo_demo"
    UNION ALL 
    SELECT DISTINCT
        week,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_rolling3mo_voc"
    UNION ALL 
    SELECT DISTINCT
        week,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_rolling3mo_closing"
    UNION ALL 
    SELECT DISTINCT
        week,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_rolling3mo_won"

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
    FROM "acton"."dbt_actonmarketing"."funnel_report_rolling3mo_leads"
    GROUP BY 2,3

), mql_base AS (
    
    SELECT
        COUNT(DISTINCT mql_id) AS mqls,
        week,
        global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_rolling3mo_mqls"
    GROUP BY 2,3

), sal_base AS (

    SELECT
        COUNT(DISTINCT sal_id) AS sals,
        week,
        global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_rolling3mo_sals"
    GROUP BY 2,3

), sql_base AS (

    SELECT
        COUNT(DISTINCT sql_id) AS sqls,
        week,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_rolling3mo_sqls"
    GROUP BY 2,3
   
), sqo_base AS (

    SELECT
        COUNT(DISTINCT sqo_id) AS sqos,
        week,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_rolling3mo_sqos"
    GROUP BY 2,3
   
), demo_base AS (

    SELECT
        COUNT(DISTINCT demo_id) AS demo,
        week,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_rolling3mo_demo"
    GROUP BY 2,3
   
), voc_base AS (

    SELECT
        COUNT(DISTINCT voc_id) AS voc,
        week,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_rolling3mo_voc"
    GROUP BY 2,3
   
), closing_base AS (

    SELECT
        COUNT(DISTINCT closing_id) AS closing,
        week,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_rolling3mo_closing"
    GROUP BY 2,3
   
), won_base AS (

    SELECT
        COUNT(DISTINCT won_id) AS won,
        week,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_rolling3mo_won"
    GROUP BY 2,3
   
), final AS (

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
        END AS sqls,
        CASE 
            WHEN SUM(sqos) IS null THEN 0
            ELSE SUM(sqos) 
        END AS sqos,
        CASE 
            WHEN SUM(demo) IS null THEN 0
            ELSE SUM(demo) 
        END AS demo,
        CASE 
            WHEN SUM(voc) IS null THEN 0
            ELSE SUM(voc) 
        END AS voc,
        CASE 
            WHEN SUM(closing) IS null THEN 0
            ELSE SUM(closing) 
        END AS closing,
        CASE 
            WHEN SUM(won) IS null THEN 0
            ELSE SUM(won) 
        END AS won
    FROM base
    LEFT JOIN lead_base ON
    base.global_region=lead_base.global_region
    AND base.week=lead_base.week
    LEFT JOIN mql_base ON
    base.global_region=mql_base.global_region
    AND base.week=mql_base.week
    LEFT JOIN sal_base ON
    base.global_region=sal_base.global_region
    AND base.week=sal_base.week
    LEFT JOIN sql_base ON
    base.global_region=sql_base.account_global_region
    AND base.week=sql_base.week
    LEFT JOIN sqo_base ON
    base.global_region=sqo_base.account_global_region
    AND base.week=sqo_base.week
    LEFT JOIN demo_base ON
    base.global_region=demo_base.account_global_region
    AND base.week=demo_base.week
    LEFT JOIN voc_base ON
    base.global_region=voc_base.account_global_region
    AND base.week=voc_base.week
    LEFT JOIN closing_base ON
    base.global_region=closing_base.account_global_region
    AND base.week=closing_base.week
    LEFT JOIN won_base ON
    base.global_region=won_base.account_global_region
    AND base.week=won_base.week
    GROUP BY 1,2
    ORDER BY 1

)

SELECT
    week,
    SUM(leads) AS leads,
    SUM(mqls) AS mqls,
    SUM(sals) AS sals,
    SUM(sqls) AS sqls,
    SUM(sqos) AS sqos,
    SUM(demo) AS demo,
    SUM(voc) AS voc, 
    SUM(closing) AS closing,
    SUM(won) AS won
FROM final
GROUP BY 1
ORDER BY 1