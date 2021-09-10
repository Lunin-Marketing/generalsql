{{ config(materialized='table') }}

WITH last_12_weeks AS (
SELECT DISTINCT
week 
FROM "acton".dbt_actonmarketing.date_base_xf
WHERE day BETWEEN CURRENT_DATE-84 AND CURRENT_DATE-7

), final AS (
    
SELECT
date_base_xf.week,
COUNT(DISTINCT contract_id) AS churned,
SUM(arr_loss_amount * -1) AS lost_customer_arr
FROM "acton".dbt_actonmarketing.contract_source_xf
LEFT JOIN "acton".dbt_actonmarketing.date_base_xf ON
contract_source_xf.churn_date=date_base_xf.day
LEFT JOIN last_12_weeks ON 
date_base_xf.week=last_12_weeks.week
WHERE 1=1
AND last_12_weeks.week IS NOT null
AND churn_date IS NOT null
AND status = 'Activated'
AND contract_status = 'Cancelled'
AND cs_churn = 'true'
GROUP BY 1

)
SELECT
week,
churned,
lost_customer_arr
FROM final