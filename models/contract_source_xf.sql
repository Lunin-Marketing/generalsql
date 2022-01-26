{{ config(materialized='table') }}

WITH base AS (
SELECT *
FROM "acton".public."Contract"

), final AS (
SELECT
"Id" AS contract_id,
"AccountId" AS account_id,
"OwnerId" AS owner_id,
"Churn_Date__c"::Date AS churn_date,
"CS_Churn__c" AS cs_churn,
"Status" AS status,
"Contract_Status__c" AS contract_status,
"ARR_Loss_Amount__c" AS arr_loss_amount,
"SystemModstamp" AS systemmodstamp,
"LastModifiedDate" AS last_modified_date
FROM base

)

SELECT
*
FROM final