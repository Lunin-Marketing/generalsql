{{ config(materialized='table') }}

SELECT
    "deploymentId" AS form_id,
    "title" AS form_title,
    "campaignSource" AS form_campaign,
    "respRecId" AS form_response_recipient_id,
    "recipientSourceId" AS form_recipient_source_id,
    "recipientId" AS form_recipient_id,
    "recipientName" AS form_recipient_name,
    "ip" AS form_ip,
    "os" AS form_os,
    "browser" AS form_browser,
    "referer" AS form_referrer,
    "cookieId" AS form_cookie_id,
    "attributeId" AS form_attribute_id,
    "respEmail" AS form_response_email,
    "userAgent" AS form_user_agent,
    "submitId" AS form_submit_id,
    "accountId" AS form_account_id,
    "verbKey" AS form_verb,
    "dateTime" AS form_date_time,
    date_trunc('day', convert_timezone('America/Los_Angeles', "insertTime"))::Date AS form_insert_time,
    "fingerprint" AS form_fingerprint 
FROM "9883_DATA".FACTS.FORM_9883