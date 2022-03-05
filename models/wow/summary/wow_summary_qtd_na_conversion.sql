{{ config(materialized='table') }}

WITH base AS (

    SELECT 
        ROUND(SUM(mqls/NULLIF(leads,0)),2) AS qtd_lead_to_mql,
        ROUND(SUM(sals/NULLIF(mqls,0)),2) AS qtd_mql_to_sal,
        ROUND(SUM(sqls/NULLIF(sals,0)),2) AS qtd_sal_to_sql,
        ROUND(SUM(sqos/NULLIF(sqls,0)),2) AS qtd_sql_to_sqo,
        ROUND(SUM(won/NULLIF(sqos,0)),2) AS qtd_sqo_to_won,
        ROUND(SUM(lost/NULLIF(sqos,0)),2) AS qtd_sqo_to_lost
    FROM {{ref('wow_summary_qtd_na')}}
   
)

SELECT *
FROM base
