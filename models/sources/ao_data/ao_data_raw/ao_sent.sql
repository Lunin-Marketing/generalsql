{{ config(materialized='table') }}

SELECT
    "fromEmail" AS message_from_email,
    "resendId" AS message_resend_id,
    "templateId" AS message_template_id,
    "adaptiveSendTime" AS message_adaptive_send_time,
    "adaptiveSendMethod" AS message_adaptive_send_method,
    "launchId" AS message_launch_id,
    "eteTags" AS message_tags,
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
FROM "9883_DATA"."FACTS"."sent_9883"