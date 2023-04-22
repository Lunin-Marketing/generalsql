{{ config(materialized='table') }}

SELECT
    "fileId" AS media_id,
    "fileName" AS media_name,
    "sourceId" AS media_source_id,
    "recipientId" AS media_recipient_id,
    "emailKey" AS media_email_key,
    "recipientName" AS media_recipient_name,
    "messageId" AS media_message_id,
    "bulkName" AS media_bulk_name,
    "ip" AS media_ip,
    "os" AS media_os,
    "browser" AS media_browser,
    "beaconCookie" AS media_cookie,
    "reportFieldsJson" AS media_report_json,
    "reportPathname" AS media_report_path,
    "userAgent" AS media_user_agent,
    "accountId" AS media_account_id,
    "verbKey" AS media_verb,
    "dateTime" AS media_date_time,
    "insertTime" AS media_insert_time,
    "fingerprint" AS media_fingerprint
FROM "9883Data".FACTS.DOWNLOAD_9883