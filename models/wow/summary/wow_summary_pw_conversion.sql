{{ config(materialized='table') }}

WITH base AS (

    SELECT 
        SUM(mqls/NULLIF(leads,0)) AS previous_lead_to_mql,
        SUM(sals/NULLIF(mqls,0)) AS previous_mql_to_sal,
        SUM(sqls/NULLIF(sals,0)) AS previous_sal_to_sql,
        SUM(sqos/NULLIF(sqls,0)) AS previous_sql_to_sqo,
        SUM(won/NULLIF(sqos,0)) AS previous_sqo_to_won,
        SUM(lost/NULLIF(sqos,0)) AS previous_sqo_to_lost,
        SUM(churn/NULLIF(sqos,0)) AS previous_sqo_to_churn
    FROM {{ref('wow_summary_pw')}}
   
)

SELECT *
FROM base
