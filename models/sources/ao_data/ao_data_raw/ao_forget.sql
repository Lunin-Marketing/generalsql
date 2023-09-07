{{ config(materialized='table') }}

SELECT
    "email" AS forget_email,
    "confirmationEmail" AS forget_confirmation_email,
    "userRequestId" AS forget_request_id,
    "cookieIdsOrRecIds" AS forget_cookie_id,
    "processingMode" AS forget_processing_mode,
    "accountId" AS forget_account_id,
    "verbKey" AS forget_verb,
    "dateTime" AS forget_date_time,
    "insertTime" AS forget_insert_time,
    "fingerprint" AS forget_fingerprint
FROM "9883_DATA"."FACTS"."forget_9883"