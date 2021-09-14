{{ config(materialized='table') }}

WITH current_week AS (
SELECT
week 
FROM "acton".dbt_actonmarketing.date_base_xf
WHERE day=CURRENT_DATE-7
), base AS (
SELECT DISTINCT
contract_source_xf.contract_id AS contract_id,
arr_loss_amount,
contract_source_xf.churn_date AS churn_date
FROM "acton".dbt_actonmarketing.contract_source_xf
LEFT JOIN "acton".dbt_actonmarketing.date_base_xf ON
contract_source_xf.churn_date=date_base_xf.day
LEFT JOIN current_week ON 
date_base_xf.week=current_week.week
WHERE current_week.week IS NOT null
AND churn_date IS NOT null
AND status = 'Activated'
AND contract_status = 'Cancelled'
AND cs_churn = 'true'
)
SELECT *
FROM base
