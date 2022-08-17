

  create  table "acton"."dbt_actonmarketing"."wow_summary_qtd_conversion2__dbt_tmp"
  as (
    

WITH base AS (

    SELECT 
        ROUND(SUM(mqls/NULLIF(leads,0)),2) AS quarterly_conv,
        '2.MQL' AS kpi,
        global_region
    FROM "acton"."dbt_actonmarketing"."wow_summary_qtd"
    GROUP BY 3
    UNION ALL
    SELECT
        ROUND(SUM(sals/NULLIF(mqls,0)),2) AS quarterly_conv,
        '3.SAL' AS kpi,
        global_region
    FROM "acton"."dbt_actonmarketing"."wow_summary_qtd"
    GROUP BY 3
    UNION ALL
    SELECT
        ROUND(SUM(sqls/NULLIF(sals,0)),2) AS quarterly_conv,
        '4.SQL' AS kpi,
        global_region
    FROM "acton"."dbt_actonmarketing"."wow_summary_qtd"
    GROUP BY 3
    UNION ALL
    SELECT
        ROUND(SUM(sqos/NULLIF(sqls,0)),2) AS quarterly_conv,
        '5.SQO' AS kpi,
        global_region
    FROM "acton"."dbt_actonmarketing"."wow_summary_qtd"
    GROUP BY 3
    UNION ALL
    SELECT      
        ROUND(SUM(won_acv/NULLIF(sqo_acv,0)),2) AS quarterly_conv,
        '10.Implement' AS kpi,
        global_region
    FROM "acton"."dbt_actonmarketing"."wow_summary_qtd"
    GROUP BY 3
   
)

SELECT *
FROM base
  );