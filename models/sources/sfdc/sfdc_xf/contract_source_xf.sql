{{ config(materialized='table') }}

WITH base AS (
SELECT *
FROM {{ source('aws_salesforce', 'contract') }}

), final AS (
SELECT
id AS contract_id,
account_id,
owner_id,
status,
is_deleted,
DATE_TRUNC('day',created_date)::Date AS created_date,
DATE_TRUNC('day',created_date)::Date AS end_date,
created_by_id,
DATE_TRUNC('day',last_modified_date)::Date AS last_modified_date,
system_modstamp AS systemmodstamp,
contract_status_c AS contract_status,
related_opportunity_c AS contract_opportunity_id,
DATE_TRUNC('day',churn_date_c)::Date AS churn_date,
cs_churn_c AS cs_churn,
arr_c AS arr,
contract_acv_c AS contract_acv,
subscription_term_c AS subscription_term,
cs_churn_recognized_mrr_c AS cs_churn_recognized_mrr,
sales_churn_c AS sales_churn,
current_quarter_debook_value_c AS current_quarter_debook_value,
CASE
    WHEN contract_status_c = 'Cancelled' AND cs_churn_c != false THEN cs_churn_recognized_mrr_c*-12
    WHEN contract_status_c = 'Cancelled' AND sales_churn_c != false THEN current_quarter_debook_value_c
    ELSE 0
END AS arr_loss_amount
FROM base
WHERE base.is_deleted = 'False'

)

SELECT
*
FROM final