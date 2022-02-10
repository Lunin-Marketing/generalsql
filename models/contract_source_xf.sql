{{ config(materialized='table') }}

WITH base AS (
SELECT *
FROM "acton".salesforce."contract"

), final AS (
SELECT
id AS contract_id,
account_id,
owner_id,
status,
is_deleted,
DATE_TRUNC('day',created_date)::Date AS created_date,
created_by_id,
DATE_TRUNC('day',last_modified_date)::Date AS last_modified_date,
system_modstamp AS systemmodstamp,
contract_status_c AS contract_status,
related_opportunity_c AS contract_opportunity_id,
DATE_TRUNC('day',churn_date)::Date AS churn_date,
cs_churn_c AS cs_churn,
arr_c AS arr
--"ARR_Loss_Amount__c" AS arr_loss_amount,
FROM base

)

SELECT
*
FROM final