{{ config(materialized='table') }}

SELECT
    "reason" AS message_failed_reason,
    "fromEmail" AS message_failed_from_email,
    "resendId" AS message_failed_resend_id,
    "failInSend" AS message_failed_total,
    "messageId" AS message_failed_message_id,
    "recipientId" AS message_failed_recipeint_id,
    "messageSubject" AS message_failed_subject,
    "recipientEmail" AS message_failed_recipient_email,
    "recipientName" AS message_failed_recipient_name,
    "sourceId" AS message_failed_source_id,
    "accountId" AS message_failed_account_id,
    "verbKey" AS message_failed_verb,
    "dateTime" AS message_failed_date_time,
    "insertTime" AS message_failed_insert_time,
    "fingerprint" AS message_failed_fingerprint
FROM "9883_DATA"."FACTS"."failed_9883"