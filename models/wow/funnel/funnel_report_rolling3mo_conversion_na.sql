{{ config(materialized='table') }}

WITH base AS (
    SELECT 
        week,
        ROUND(SUM(mqls/NULLIF(leads,0)),2) AS lead_to_mql,
        ROUND(SUM(sals/NULLIF(mqls,0)),2) AS mql_to_sal,
        ROUND(SUM(sqls/NULLIF(sals,0)),2) AS sal_to_sql,
        ROUND(SUM(sqos/NULLIF(sqls,0)),2) AS sql_to_sqo,
        ROUND(SUM(won/NULLIF(sqos,0)),2) AS sqo_to_won
    FROM {{ref('funnel_report_rolling3mo_combined_na')}}
    GROUP BY 1
)

SELECT *
FROM base