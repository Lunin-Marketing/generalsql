{{ config(materialized='table') }}

SELECT
    "pubId" AS social_pub_id,
    "title" AS social_pub_title,
    "channelType" AS social_pub_channel_type,
    "personaId" AS social_pub_persona_id,
    "personaName" AS social_pub_persona_name,
    "pageId" AS social_pub_page_id,
    "pageName" AS social_pub_page_name,
    "assetId" AS social_pub_asset_id,
    "ip" AS social_pub_ip,
    "os" AS social_pub_os,
    "browser" AS social_pub_browser,
    "block" AS social_pub_block,
    "data" AS social_pub_data,
    "userAgent" AS social_pub_user_agent,
    "postAsPage" AS social_pub_post_page,
    "cookieId" AS social_pub_cookie_id,
    "accountId" AS social_pub_account_id,
    "verbKey" AS social_pub_verb,
    "dateTime" AS social_pub_date_time,
    "insertTime" AS social_pub_insert_time,
    "fingerprint" AS social_pub_fingerprint 
FROM "9883_DATA"."FACTS"."socialPubResponse_9883"