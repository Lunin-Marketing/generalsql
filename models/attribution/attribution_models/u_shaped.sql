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

), first_touch_final AS (

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
        'first_touch' AS touchpoint_position,
        .5 AS u_shaped_weight_prep
    FROM first_touch_base
    WHERE touchpoint_number = 1

), lead_creation_base AS (

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

), lead_creation_final AS (

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
        'lead_creation_touch' AS touchpoint_position,
        .5 AS u_shaped_weight_prep
    FROM lead_creation_base
    WHERE touchpoint_number = 1

), unioned AS (

    SELECT *
    FROM first_touch_final
    UNION ALL
    SELECT *
    FROM lead_creation_final
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
    touchpoint_position,
    SUM(u_shaped_weight_prep) AS u_shaped_weight
FROM unioned
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15