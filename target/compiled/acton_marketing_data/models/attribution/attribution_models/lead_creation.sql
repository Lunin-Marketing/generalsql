

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
    WHERE asset_type = 'form'

), person_source_prep AS (

    SELECT
        email,
        cast(created_date::Timestamp at time zone 'UTC' at time zone 'America/Los_Angeles' as TIMESTAMP) AS created_date
    FROM "acton"."dbt_actonmarketing"."person_source_xf"

), person_source AS (

    SELECT
        email,
        created_date::Date AS created_date
    FROM person_source_prep

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
        ROW_NUMBER() OVER (PARTITION BY base.email ORDER BY action_time ASC ) AS touchpoint_number
    FROM base
    LEFT JOIN person_source person ON
    base.email=person.email
    WHERE action_day=person.created_date

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
    'LC' AS lc_position,
    1 AS lead_creation_weight,
    .5 AS u_shaped_weight,
    .3 AS w_shaped_weight,
    .225 AS full_path_weight
FROM lead_creation_base
WHERE touchpoint_number = 1