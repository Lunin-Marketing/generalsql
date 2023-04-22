{{ config(materialized='table') }}

SELECT
    "messageId" AS message_id,
    "origin" AS message_origin,
    "recipientId" AS message_recipient_id,
    "bounEmailKey" AS message_email_key,
    "bounName" AS message_email_name,
    "sourceId" AS message_source_id,
    "bounEmail" AS message_email,
    "category" AS message_category,
    "diagnostic" AS message_diagnostic,
    "launchId" AS message_launch_id,
    "accountId" AS message_account_id,
    "verbKey" AS message_verb,
    "dateTime" AS message_date_time,
    "insertTime" AS message_insert_time,
    "fingerprint" AS message_fingerprint    
FROM "9883Data".FACTS.BOUN_9883