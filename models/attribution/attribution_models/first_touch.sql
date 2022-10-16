{{ config(materialized='table') }}

WITH base AS (

    SELECT 
        action,
        action_time,
        cookie_id,
        asset_id,
        asset_title,
        ip_address,
        record_id,
        referral_url,
        response_email,
        action_day,
        unique_visitor_id,
        email,
        touchpoint_id,
        asset_type
    FROM {{ref('ao_combined')}}

), first_touch_base AS (

    SELECT
        email,
        action,
        action_time,
        cookie_id,
        asset_id,
        asset_title,
        ip_address,
        record_id,
        referral_url,
        response_email,
        action_day,
        unique_visitor_id,
        touchpoint_id,
        asset_type,
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY action_time ) AS touchpoint_number
    FROM base

)

SELECT
    email,
    action,
    action_time,
    cookie_id,
    asset_id,
    asset_title,
    ip_address,
    record_id,
    referral_url,
    response_email,
    action_day,
    unique_visitor_id,
    touchpoint_id,
    asset_type,
    '1' AS first_touch_weight
FROM first_touch_base
WHERE touchpoint_number = 1