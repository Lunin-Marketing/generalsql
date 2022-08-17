

WITH base AS (

    SELECT 
        ROUND(SUM(mqls)/NULLIF(SUM(leads),0),2) AS weekly_conv,
        '2.MQL' AS kpi,
        global_region
    FROM "acton"."dbt_actonmarketing"."wow_summary_pw"
    GROUP BY 3
    UNION ALL
    SELECT
        ROUND(SUM(sals)/NULLIF(SUM(mqls),0),2) AS weekly_conv,
        '3.SAL' AS kpi,
        global_region
    FROM "acton"."dbt_actonmarketing"."wow_summary_pw"
    GROUP BY 3
    UNION ALL
    SELECT
        ROUND(SUM(sqls)/NULLIF(SUM(sals),0),2) AS weekly_conv,
        '4.SQL' AS kpi,
        global_region
    FROM "acton"."dbt_actonmarketing"."wow_summary_pw"
    GROUP BY 3
    UNION ALL
    SELECT
        ROUND(SUM(sqos)/NULLIF(SUM(sqls),0),2) AS weekly_conv,
        '5.SQO' AS kpi,
        global_region
    FROM "acton"."dbt_actonmarketing"."wow_summary_pw"
    GROUP BY 3
    UNION ALL
    SELECT      
        ROUND(SUM(won_acv)/NULLIF(SUM(sqo_acv),0),2) AS weekly_conv,
        '10.Implement' AS kpi,
        global_region
    FROM "acton"."dbt_actonmarketing"."wow_summary_pw"
    GROUP BY 3
    UNION ALL
    SELECT 
        ROUND(SUM(mqls)/NULLIF(SUM(leads),0),2) AS weekly_conv,
        '2.MQL' AS kpi,
        'Global' AS global_region
    FROM "acton"."dbt_actonmarketing"."wow_summary_pw"
    GROUP BY 3
    UNION ALL
    SELECT
        ROUND(SUM(sals)/NULLIF(SUM(mqls),0),2) AS weekly_conv,
        '3.SAL' AS kpi,
        'Global' AS global_region
    FROM "acton"."dbt_actonmarketing"."wow_summary_pw"
    GROUP BY 3
    UNION ALL
    SELECT
        ROUND(SUM(sqls)/NULLIF(SUM(sals),0),2) AS weekly_conv,
        '4.SQL' AS kpi,
        'Global' AS global_region
    FROM "acton"."dbt_actonmarketing"."wow_summary_pw"
    GROUP BY 3
    UNION ALL
    SELECT
        ROUND(SUM(sqos)/NULLIF(SUM(sqls),0),2) AS weekly_conv,
        '5.SQO' AS kpi,
        'Global' AS global_region
    FROM "acton"."dbt_actonmarketing"."wow_summary_pw"
    GROUP BY 3
    UNION ALL
    SELECT      
        ROUND(SUM(won_acv)/NULLIF(SUM(sqo_acv),0),2) AS weekly_conv,
        '10.Implement' AS kpi,
        'Global' AS global_region
    FROM "acton"."dbt_actonmarketing"."wow_summary_pw"
    GROUP BY 3
)

SELECT *
FROM base