{{ config(materialized='table') }}

SELECT
    "ip" AS message_ip,
    "os" AS message_os,
    "browser" AS message_browser,
    "blockName" AS message_block_name,
    "linkName" AS message_link_name,
    "userAgent" AS message_user_agent,
    "templateId" AS message_template_id,
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
FROM "9883Data".FACTS.RESPONSE_9883