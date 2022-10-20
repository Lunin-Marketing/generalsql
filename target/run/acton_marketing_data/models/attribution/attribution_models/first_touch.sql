

  create  table "acton"."dbt_actonmarketing"."first_touch__dbt_tmp"
  as (
    

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

)

SELECT
    email,
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
    'FT' AS ft_position,
    1 AS first_touch_weight,
    .5 AS u_shaped_weight,
    .3 AS w_shaped_weight,
    .225 AS full_path_weight
FROM first_touch_base
WHERE touchpoint_number = 1
  );