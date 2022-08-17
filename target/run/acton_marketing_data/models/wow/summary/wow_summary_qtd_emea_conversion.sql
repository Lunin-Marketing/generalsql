

  create  table "acton"."dbt_actonmarketing"."wow_summary_qtd_emea_conversion__dbt_tmp"
  as (
    

WITH base AS (

    SELECT 
        ROUND(SUM(mqls/NULLIF(leads,0)),2) AS qtd_lead_to_mql,
        ROUND(SUM(sals/NULLIF(mqls,0)),2) AS qtd_mql_to_sal,
        ROUND(SUM(sqls/NULLIF(sals,0)),2) AS qtd_sal_to_sql,
        ROUND(SUM(sqos/NULLIF(sqls,0)),2) AS qtd_sql_to_sqo,
        ROUND(SUM(won/NULLIF(sqos,0)),2) AS qtd_sqo_to_won,
        ROUND(SUM(lost/NULLIF(sqos,0)),2) AS qtd_sqo_to_lost
    FROM "acton"."dbt_actonmarketing"."wow_summary_qtd_emea"
   
)

SELECT *
FROM base
  );