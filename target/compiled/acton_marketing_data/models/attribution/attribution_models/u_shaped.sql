

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
        ROW_NUMBER() OVER (PARTITION BY email ORDER BY action_time ASC) AS touchpoint_number
    FROM base

), first_touch_final AS (

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
        'first_touch' AS touchpoint_position,
        .5 AS u_shaped_weight_prep
    FROM first_touch_base
    WHERE touchpoint_number = 1

), lead_creation_base AS (

      SELECT
        touchpoint_id,
        action,
        action_time,
        action_day,
        asset_id,
        base.email,
        asset_title,
        subject_line,
        from_address,
        clicked_url,
        clickthrough_link_name,
        referral_url,
        event_id,
        asset_type,
        ROW_NUMBER() OVER (PARTITION BY base.email ORDER BY action_time ASC) AS touchpoint_number
    FROM base
    LEFT JOIN "acton"."dbt_actonmarketing"."person_source_xf" person ON
    base.email=person.email
    WHERE action_day=person.created_date

), lead_creation_final AS (

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
    touchpoint_position,
    SUM(u_shaped_weight_prep) AS u_shaped_weight
FROM unioned
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15