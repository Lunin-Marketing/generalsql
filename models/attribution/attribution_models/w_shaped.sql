{{ config(materialized='table') }}

WITH base AS (

    SELECT 
        email, 
        person_id, 
        account_id, 
        opportunity_id, 
        CAST(net_arr AS numeric) AS net_arr, 
        opportunity_creation_date,
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
        asset_type,
        touchpoint_id
    FROM {{ref('ao_combined')}}

), first_touch_base AS (

    SELECT
        email, 
        person_id, 
        account_id, 
        opportunity_id, 
        net_arr, 
        opportunity_creation_date,
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
        asset_type,
        touchpoint_id,
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY action_time ) AS touchpoint_number
    FROM base

), first_touch_final AS (

    SELECT *,
    'first_touch' AS touchpoint_type,
    .3 AS w_shaped_weight
    FROM first_touch_base
    WHERE touchpoint_number=1

), lead_creation_base AS (

    SELECT
        email, 
        person_id, 
        account_id, 
        opportunity_id, 
        net_arr, 
        opportunity_creation_date,
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
        asset_type,
        touchpoint_id,
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY action_time ) AS touchpoint_number
    FROM base

), lead_creation_final AS (

    SELECT *,
    'lead_creation_touch' AS touchpoint_type,
    .3 AS w_shaped_weight
    FROM lead_creation_base
    WHERE touchpoint_number=1

), opportunity_creation_base AS (

    SELECT
        email, 
        person_id, 
        account_id, 
        opportunity_id, 
        net_arr, 
        opportunity_creation_date,
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
        asset_type,
        touchpoint_id,
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY action_time ) AS touchpoint_number
    FROM base
    WHERE opportunity_creation_date::Date <= action_time::Date

), opportunity_creation_final AS (

    SELECT *,
        'opportunity_creation_touch' AS touchpoint_type,
        .3 AS w_shaped_weight
    FROM opportunity_creation_base
    WHERE touchpoint_number = 1

), unioned AS (

    SELECT *
    FROM first_touch_final
    UNION ALL
    SELECT *
    FROM lead_creation_final
    UNION ALL
    SELECT *
    FROM opportunity_creation_final

)

SELECT 
    email, 
    person_id, 
    account_id, 
    opportunity_id, 
    net_arr, 
    opportunity_creation_date,
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
    asset_type,
    touchpoint_id,
    touchpoint_type,
    w_shaped_weight,
    SUM(net_arr*w_shaped_weight) AS w_shaped_net_arr
FROM unioned
{{ dbt_utils.group_by(n=21) }}