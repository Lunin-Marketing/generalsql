

WITH base AS (

    SELECT DISTINCT
        global_region,
        company_size_rev,
        date,
        ROUND(SUM(mqls/NULLIF(leads,0)),2) AS all_time_lead_to_mql,
        ROUND(SUM(sals/NULLIF(mqls,0)),2) AS all_time_mql_to_sal,
        ROUND(SUM(sqls/NULLIF(sals,0)),2) AS all_time_sal_to_sql,
        ROUND(SUM(sqos/NULLIF(sqls,0)),2) AS all_time_sql_to_sqo,
        ROUND(SUM(demos/NULLIF(sqos,0)),2) AS all_time_sqo_to_demo,
        ROUND(SUM(vocs/NULLIF(demos,0)),2) AS all_time_demo_to_voc,
        ROUND(SUM(closing/NULLIF(vocs,0)),2) AS all_time_voc_to_closing,
        ROUND(SUM(won/NULLIF(closing,0)),2) AS all_time_closing_to_won,
        ROUND(SUM(lost/NULLIF(closing,0)),2) AS all_time_closing_to_lost
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_all"
    GROUP BY 1,2,3
    
)

SELECT DISTINCT
    global_region,
    company_size_rev,
    date,
    CASE 
        WHEN all_time_lead_to_mql IS null THEN 0
        ELSE all_time_lead_to_mql
    END AS all_time_lead_to_mql,
    CASE 
        WHEN all_time_mql_to_sal IS null THEN 0
        ELSE all_time_mql_to_sal
    END AS all_time_mql_to_sal,
    CASE 
        WHEN all_time_sal_to_sql IS null THEN 0
        ELSE all_time_sal_to_sql
    END AS all_time_sal_to_sql,
    CASE 
        WHEN all_time_sql_to_sqo IS null THEN 0
        ELSE all_time_sql_to_sqo
    END AS all_time_sql_to_sqo,
    CASE 
        WHEN all_time_sqo_to_demo IS null THEN 0
        ELSE all_time_sqo_to_demo
    END AS all_time_sqo_to_demo,
    CASE 
        WHEN all_time_demo_to_voc IS null THEN 0
        ELSE all_time_demo_to_voc
    END AS all_time_demo_to_voc,
    CASE 
        WHEN all_time_voc_to_closing IS null THEN 0
        ELSE all_time_voc_to_closing
    END AS all_time_voc_to_closing,
    CASE 
        WHEN all_time_closing_to_won IS null THEN 0
        ELSE all_time_closing_to_won
    END AS all_time_closing_to_won,
    CASE 
        WHEN all_time_closing_to_lost IS null THEN 0
        ELSE all_time_closing_to_lost
    END AS all_time_closing_to_lost
FROM base