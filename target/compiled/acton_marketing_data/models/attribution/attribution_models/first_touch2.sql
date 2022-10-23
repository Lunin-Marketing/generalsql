

WITH base AS (

    SELECT 
        touchpoint_id,
        action,
        action_time,
        action_day,
        asset_id,
        email,
        asset_title,
        subject_line,
        from_address,
        clicked_url,
        clickthrough_link_name,
        referral_url,
        event_id,
        asset_type
    FROM "acton"."dbt_actonmarketing"."ao_combined"

), first_touch_base AS (

    SELECT
        touchpoint_id,
        action,
        action_time,
        action_day,
        asset_id,
        email,
        asset_title,
        subject_line,
        from_address,
        clicked_url,
        clickthrough_link_name,
        referral_url,
        event_id,
        asset_type,
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY action_time ASC ) AS touchpoint_number
    FROM base

), linear_base AS (

    SELECT
        email,
        COUNT(DISTINCT touchpoint_id) AS linear_touches
    FROM base
    GROUP BY 1
)

SELECT
    first_touch_base.email,
    touchpoint_id,
    action,
    action_time,
    action_day,
    asset_id,
    asset_title,
    subject_line,
    from_address,
    clicked_url,
    clickthrough_link_name,
    referral_url,
    event_id,
    asset_type,
    touchpoint_number,
    linear_touches,
    CASE
        WHEN touchpoint_number = 1 THEN 'FT' 
        ELSE null
    END AS ft_position,
    CASE
        WHEN touchpoint_number = 1 THEN 1
        ELSE 0
    END AS first_touch_weight,
    CASE
        WHEN touchpoint_number = 1 THEN .5
        ELSE 0
    END AS u_shaped_weight,
    CASE
        WHEN touchpoint_number = 1 THEN .3 
        WHEN linear_touches = 1 THEN .1
        ELSE (.1/SUM(linear_touches-1))
    END AS w_shaped_weight,
    CASE
        WHEN touchpoint_number = 1 THEN .225 
        WHEN linear_touches = 1 THEN .1
        ELSE (.1/SUM(linear_touches-1))
    END AS full_path_weight
FROM first_touch_base
LEFT JOIN linear_base ON
first_touch_base.email=linear_base.email
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19