{{ config(materialized='table') }}
WITH base AS (
SELECT *
FROM "acton".public."Task"

), final AS (
SELECT
"Id" AS task_id,
"RecordTypeId" AS record_type,
"WhoId" AS who_id,
"WhatId" AS what_id,
"Subject" AS subject,
"ActivityDate" AS activity_date,
"Status" AS status,
"OwnerId" AS owner_id,
"Type" AS type,
"IsDeleted" AS is_deleted,
"AccountId" AS account_id,
"IsClosed" AS is_closed,
"CreatedDate" AS task_created_date
FROM base
)
SELECT*
FROM final
