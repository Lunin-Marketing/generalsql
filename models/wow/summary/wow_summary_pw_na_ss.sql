{{ config(materialized='table') }}

WITH base_prep AS (

    SELECT DISTINCT
        global_region
    FROM {{ref('funnel_report_previous_week_leads_ss')}}
    UNION ALL 
    SELECT DISTINCT
        global_region
    FROM {{ref('funnel_report_previous_week_mqls_ss')}}
    UNION ALL 
    SELECT DISTINCT
        global_region
    FROM {{ref('funnel_report_previous_week_sals_ss')}}
    UNION ALL 
    SELECT DISTINCT
        account_global_region
    FROM {{ref('funnel_report_previous_week_sqls_ss')}}
    UNION ALL 
    SELECT DISTINCT
        account_global_region
    FROM {{ref('funnel_report_previous_week_sqos_ss')}}
    UNION ALL 
    SELECT DISTINCT
        account_global_region
    FROM {{ref('funnel_report_previous_week_demo_ss')}}
    UNION ALL 
    SELECT DISTINCT
        account_global_region
    FROM {{ref('funnel_report_previous_week_voc_ss')}}
    UNION ALL 
    SELECT DISTINCT
        account_global_region
    FROM {{ref('funnel_report_previous_week_closing_ss')}}
    UNION ALL 
    SELECT DISTINCT
        account_global_region
    FROM {{ref('funnel_report_previous_week_won')}}
    UNION ALL 
    SELECT DISTINCT
        account_global_region
    FROM {{ref('funnel_report_previous_week_lost')}}

), base AS (

    SELECT DISTINCT
    global_region
    FROM base_prep

), lead_base AS (

    SELECT
        COUNT(DISTINCT lead_id) AS leads,
        global_region
    FROM {{ref('funnel_report_previous_week_leads_ss')}}
    GROUP BY 2

), mql_base AS (
    
    SELECT
        COUNT(DISTINCT mql_id) AS mqls,
        global_region
    FROM {{ref('funnel_report_previous_week_mqls_ss')}}
    GROUP BY 2

), sal_base AS (

    SELECT
        COUNT(DISTINCT sal_id) AS sals,
        global_region
    FROM {{ref('funnel_report_previous_week_sals_ss')}}
    GROUP BY 2

), sql_base AS (

    SELECT
        COUNT(DISTINCT sql_id) AS sqls,
        account_global_region
    FROM {{ref('funnel_report_previous_week_sqls_ss')}}
    GROUP BY 2
   
), sqo_base AS (

    SELECT
        COUNT(DISTINCT sqo_id) AS sqos,
        account_global_region
    FROM {{ref('funnel_report_previous_week_sqos_ss')}}
    GROUP BY 2
   
), demo_base AS (

    SELECT
        COUNT(DISTINCT demo_id) AS demo,
        account_global_region
    FROM {{ref('funnel_report_previous_week_demo_ss')}}
    GROUP BY 2
   
), voc_base AS (

    SELECT
        COUNT(DISTINCT voc_id) AS voc,
        account_global_region
    FROM {{ref('funnel_report_previous_week_voc_ss')}}
    GROUP BY 2
   
), closing_base AS (

    SELECT
        COUNT(DISTINCT closing_id) AS closing,
        account_global_region
    FROM {{ref('funnel_report_previous_week_closing_ss')}}
    GROUP BY 2
   
), won_base AS (

    SELECT
        COUNT(DISTINCT won_id) AS won,
        account_global_region
    FROM {{ref('funnel_report_previous_week_won')}}
    GROUP BY 2
   
), lost_base AS (

    SELECT
        COUNT(DISTINCT lost_id) AS lost,
        account_global_region
    FROM {{ref('funnel_report_previous_week_lost')}}
    GROUP BY 2
   
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
        END AS won,
        CASE 
            WHEN SUM(lost) IS null THEN 0
            ELSE SUM(lost) 
        END AS lost
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
SUM(demo) AS demo,
SUM(voc) AS voc, 
SUM(closing) AS closing,
SUM(won) AS won,
SUM(lost) AS lost
FROM final
WHERE global_region NOT IN ('EUROPE','APJ','AUNZ','ROW')