{{ config(materialized='table') }}

SELECT
    "messageId" AS message_id,
    "recipientId" AS message_recipient_id,
    "recipientEmail" AS message_recipient_email,
    "respName" AS message_recipient_name,
    "sourceId" AS message_source_id,
    "ip" AS message_ip,
    "os" AS message_os,
    "browser" AS message_browser,
    "userAgent" AS message_user_agent,
    "engagedTime" AS message_engaged_time,
    "templateId" AS message_template_id,
    "launchId" AS message_launch_id,
    "eteTags" AS message_tags,
    "accountId" AS message_account_id,
    "verbKey" AS message_verb,
    "dateTime" AS message_date_time,
    "insertTime" AS message_insert_time,
    "fingerprint" AS message_fingerprint 
FROM "9883Data".FACTS.OPEN_9883