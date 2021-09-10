{{ config(materialized='table') }}

WITH last_week AS (
SELECT DISTINCT
week 
FROM "acton".dbt_actonmarketing.date_base_xf
WHERE day = CURRENT_DATE-7

), final AS (
    
SELECT
date_base_xf.week,
COUNT(DISTINCT contract_id) AS churned
FROM "acton".dbt_actonmarketing.contract_source_xf
LEFT JOIN "acton".dbt_actonmarketing.date_base_xf ON
contract_source_xf.churn_date=date_base_xf.day
LEFT JOIN last_week ON 
date_base_xf.week=last_week.week
WHERE last_week.week IS NOT null
AND churn_date IS NOT null
AND status = 'Activated'
AND contract_status = 'Cancelled'
AND cs_churn = 'true'
GROUP BY 1

)
SELECT
week,
churned
FROM final