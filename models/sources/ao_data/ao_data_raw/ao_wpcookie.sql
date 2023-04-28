{{ config(materialized='table') }}

SELECT
    "cookieId" AS cookie_id,
    "cookieOldId" AS cookie_old_id,
    "cookieOldEmail" AS cookie_old_email,
    "cookieOldName" AS cookie_old_name,
    "cookieOldSourceType" AS cookie_old_source_type,
    "cookieOldSourceId" AS cookie_old_source_id,
    "cookieNewId" AS cookie_new_id,
    "cookieNewEmail" AS cookie_new_email,
    "cookieNewName" AS cookie_new_name,
    "cookieNewSourceType" AS cookie_new_source_type,
    "cookieNewSourceId" AS cookie_new_source_id,
    "cookieNewRequestIp" AS cookie_new_ip,
    "accountId" AS cookie_account_id,
    "verbKey" AS cookie_verb,
    "dateTime" AS cookie_date_time,
    "insertTime" AS cookie_insert_time,
    "fingerprint" AS cookie_fingerprint 
FROM "9883_DATA".FACTS.WPCOOKIE_9883