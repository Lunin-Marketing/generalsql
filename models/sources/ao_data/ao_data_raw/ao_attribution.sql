{{ config(materialized='table') }}

SELECT
    "attributionId" AS attribution_id,
    "attributionCookieId" AS attribution_cookie_id,
    "attributionType" AS attribution_type,
    "attributionSourceId" AS attribution_source_id,
    "attributionReferringDomain" AS attribution_referring_domain,
    "attributionKeywords" AS attribution_keywords,
    "attributionExtra" AS attribution_extra,
    "attributionMatchType" AS attribution_match_type,
    "attributionNetworkAddress" AS attribution_network_address,
    "attributionUserAgent" AS attribution_user_agent,
    "attributionReferrerUrl" AS attribution_referrer_url,
    "attributionEntryUrl" AS attribution_entry_url,
    "accountId" AS attribution_account_id,
    "verbKey" AS attribution_verb,
    "dateTime" AS attribution_datetime,
    "insertTime" AS attribution_insert_time,
    "fingerprint" AS attribution_fingerprint
FROM "9883_DATA"."FACTS"."attribution_9883"