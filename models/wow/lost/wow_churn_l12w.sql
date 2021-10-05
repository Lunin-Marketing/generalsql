{{ config(materialized='table') }}

WITH last_12_weeks AS (
SELECT DISTINCT
week 
FROM "acton".dbt_actonmarketing.date_base_xf
WHERE day BETWEEN CURRENT_DATE-84 AND CURRENT_DATE-7

), churn_final AS (
SELECT
churn_date,
contract_id,
arr_loss_amount
FROM "acton".dbt_actonmarketing.contract_source_xf
WHERE 1=1
AND churn_date IS NOT null
AND status = 'Activated'
AND contract_status = 'Cancelled'
AND cs_churn = 'true'
), final AS (
    
SELECT
last_12_weeks.week,
COUNT(DISTINCT contract_id) AS churned,
SUM(arr_loss_amount) AS lost_customer_arr
FROM last_12_weeks
LEFT JOIN "acton".dbt_actonmarketing.date_base_xf ON
date_base_xf.week=last_12_weeks.week
LEFT JOIN churn_final ON 
churn_final.churn_date=date_base_xf.day
WHERE 1=1
AND last_12_weeks.week IS NOT null
GROUP BY 1

)
SELECT
week,
churned,
lost_customer_arr
FROM final
ORDER BY 1 ASC