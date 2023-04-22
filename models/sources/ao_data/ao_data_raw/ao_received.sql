{{ config(materialized='table') }}

SELECT
    "recipientEmailDuplicate" AS message_email_duplicate,
    "messageId" AS message_id,
    "recipientId" AS message_recipient_id,
    "messageSubject" AS message_subject,
    "recipientEmail" AS message_recipient_email,
    "recipientName" AS message_recipient_name,
    "sourceId" AS message_source_id,
    "accountId" AS message_account_id,
    "verbKey" AS message_verb,
    "dateTime" AS message_date_time,
    "insertTime" AS message_insert_time,
    "fingerprint" AS message_fingerprint 
FROM "9883Data".FACTS.RECEIVED_9883