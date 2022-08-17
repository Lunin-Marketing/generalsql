

  create  table "acton"."dbt_actonmarketing"."wow_summary_lw_na_ss__dbt_tmp"
  as (
    

WITH base_prep AS (

    SELECT DISTINCT
        global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_week_leads_ss"
    UNION ALL 
    SELECT DISTINCT
        global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_week_mqls_ss"
    UNION ALL 
    SELECT DISTINCT
        global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_week_sals_ss"
    UNION ALL 
    SELECT DISTINCT
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_week_sqls_ss"
    UNION ALL 
    SELECT DISTINCT
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_week_sqos_ss"
    UNION ALL 
    SELECT DISTINCT
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_week_demo_ss"
    UNION ALL 
    SELECT DISTINCT
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_week_voc_ss"
    UNION ALL 
    SELECT DISTINCT
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_week_closing_ss"
    UNION ALL 
    SELECT DISTINCT
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_week_won"
    UNION ALL 
    SELECT DISTINCT
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_week_lost"

), base AS (

    SELECT DISTINCT
    global_region
    FROM base_prep

), lead_base AS (

    SELECT
        COUNT(DISTINCT lead_id) AS leads,
        global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_week_leads_ss"
    GROUP BY 2

), mql_base AS (
    
    SELECT
        COUNT(DISTINCT mql_id) AS mqls,
        global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_week_mqls_ss"
    GROUP BY 2

), sal_base AS (

    SELECT
        COUNT(DISTINCT sal_id) AS sals,
        global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_week_sals_ss"
    GROUP BY 2

), sql_base AS (

    SELECT
        COUNT(DISTINCT sql_id) AS sqls,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_week_sqls_ss"
    GROUP BY 2
   
), sqo_base AS (

    SELECT
        COUNT(DISTINCT sqo_id) AS sqos,
        SUM(acv) AS sqo_acv,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_week_sqos_ss"
    GROUP BY 3
   
), demo_base AS (

    SELECT
        COUNT(DISTINCT demo_id) AS demo,
        SUM(acv) AS demo_acv,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_week_demo_ss"
    GROUP BY 3
   
), voc_base AS (

    SELECT
        COUNT(DISTINCT voc_id) AS voc,
        SUM(acv) AS voc_acv,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_week_voc_ss"
    GROUP BY 3
   
), closing_base AS (

    SELECT
        COUNT(DISTINCT closing_id) AS closing,
        SUM(acv) AS closing_acv,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_week_closing_ss"
    GROUP BY 3
   
), won_base AS (

    SELECT
        COUNT(DISTINCT won_id) AS won,
        SUM(acv) AS won_acv,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_week_won"
    GROUP BY 3
   
), lost_base AS (

    SELECT
        COUNT(DISTINCT lost_id) AS lost,
        SUM(acv_deal_size_usd) AS lost_acv,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_week_lost"
    GROUP BY 3
   
), final AS (

    SELECT
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
            WHEN SUM(sqo_acv) IS null THEN 0
            ELSE SUM(sqo_acv) 
        END AS sqo_acv,
        CASE 
            WHEN SUM(demo) IS null THEN 0
            ELSE SUM(demo) 
        END AS demo,
        CASE 
            WHEN SUM(demo_acv) IS null THEN 0
            ELSE SUM(demo_acv) 
        END AS demo_acv,
        CASE 
            WHEN SUM(voc) IS null THEN 0
            ELSE SUM(voc) 
        END AS voc,
        CASE 
            WHEN SUM(voc_acv) IS null THEN 0
            ELSE SUM(voc_acv) 
        END AS voc_acv,
        CASE 
            WHEN SUM(closing) IS null THEN 0
            ELSE SUM(closing) 
        END AS closing,
        CASE 
            WHEN SUM(closing_acv) IS null THEN 0
            ELSE SUM(closing_acv) 
        END AS closing_acv,
        CASE 
            WHEN SUM(won) IS null THEN 0
            ELSE SUM(won) 
        END AS won,
        CASE 
            WHEN SUM(won_acv) IS null THEN 0
            ELSE SUM(won_acv) 
        END AS won_acv,
        CASE 
            WHEN SUM(lost) IS null THEN 0
            ELSE SUM(lost) 
        END AS lost,
        CASE 
            WHEN SUM(lost_acv) IS null THEN 0
            ELSE SUM(lost_acv) 
        END AS lost_acv
    FROM base
    LEFT JOIN lead_base ON
    base.global_region=lead_base.global_region
    LEFT JOIN mql_base ON
    base.global_region=mql_base.global_region
    LEFT JOIN sal_base ON
    base.global_region=sal_base.global_region
    LEFT JOIN sql_base ON
    base.global_region=sql_base.account_global_region
    LEFT JOIN sqo_base ON
    base.global_region=sqo_base.account_global_region
    LEFT JOIN demo_base ON
    base.global_region=demo_base.account_global_region
    LEFT JOIN voc_base ON
    base.global_region=voc_base.account_global_region
    LEFT JOIN closing_base ON
    base.global_region=closing_base.account_global_region
    LEFT JOIN won_base ON
    base.global_region=won_base.account_global_region
    LEFT JOIN lost_base ON
    base.global_region=lost_base.account_global_region
    GROUP BY 1
    ORDER BY 1

)

SELECT
SUM(leads) AS leads,
SUM(mqls) AS mqls,
SUM(sals) AS sals,
SUM(sqls) AS sqls,
SUM(sqos) AS sqos,
SUM(sqo_acv) AS sqo_acv,
SUM(demo) AS demo,
SUM(demo_acv) AS demo_acv,
SUM(voc) AS voc, 
SUM(voc_acv) AS voc_acv,
SUM(closing) AS closing,
SUM(closing_acv) AS closing_acv,
SUM(won) AS won,
SUM(won_acv) AS won_acv,
SUM(lost) AS lost,
SUM(lost_acv) AS lost_acv
FROM final
WHERE global_region NOT IN ('EUROPE','APJ','AUNZ','ROW')
  );