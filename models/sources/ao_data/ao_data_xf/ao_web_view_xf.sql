{{ config(materialized='table') }}

WITH ao_beacon_base AS (

    SELECT *
    FROM {{ref('ao_beacon')}}

), web_email_base AS (

    SELECT DISTINCT
        LOWER(beacon_email) AS email,
        beacon_cookie_id
    FROM ao_beacon_base
    WHERE beacon_email IS NOT null
    UNION ALL
    SELECT DISTINCT
        LOWER(cookie_new_email) AS email,
        cookie_id
    FROM {{ref('ao_wpcookie')}}

), final AS (

    SELECT DISTINCT
        beacon_recipient_id,
        web_email_base.email AS beacon_email,
        beacon_recipient_name,
        beacon_page,
        beacon_ip_address,
        beacon_os,
        beacon_browser,
        beacon_source_type,
        beacon_source_id,
        beacon_referrer,
        beacon_cookie_source_type,
        beacon_cookie_source_id,
        beacon_social_type,
        beacon_social_id,
        beacon_social_handle,
        ao_beacon_base.beacon_cookie_id,
        beacon_attribution_id,
        beacon_geo,
        beacon_org,
        beacon_user_agent,
        beacon_longitude,
        beacon_latitude,
        beacon_area_code,
        beacon_postal_code,
        beacon_account_id,
        beacon_verb,
        beacon_datetime,
        beacon_insert_time 
    FROM ao_beacon_base
    LEFT JOIN web_email_base
        ON ao_beacon_base.beacon_cookie_id=web_email_base.beacon_cookie_id
    WHERE web_email_base.email IS NOT null

)

SELECT *
FROM final
-- WHERE beacon_email = 'bwingate@lohfeldconsulting.com'
-- AND beacon_verb = 'h'
ORDER BY beacon_datetime DESC