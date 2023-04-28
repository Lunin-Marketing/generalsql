{{ config(materialized='table') }}

SELECT
    "messageId" AS message_id,
    "origin" AS message_origin,
    "recipientId" AS message_recipient_id,
    "inouEmailKey" AS message_email_key,
    "recipientName" AS message_recipient_name,
    "sourceId" AS message_source_id,
    "inouEmail" AS message_email,
    "accountId" AS message_account_id,
    "verbKey" AS message_verb,
    "dateTime" AS message_date_time,
    "insertTime" AS message_insert_time,
    "fingerprint" AS message_fingerprint 
FROM "9883_DATA".FACTS.INOU_9883