{{ config(materialized='table') }}

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
    FROM {{ref('ao_combined')}}

), opp_creation_base AS (

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
        ROW_NUMBER() OVER (PARTITION BY cohort.opportunity_id ORDER BY action_time ASC ) AS touchpoint_number
    FROM base
    LEFT JOIN {{ref('lead_to_cw_cohort')}} cohort ON
    base.email=cohort.email
    WHERE action_day=cohort.opp_created_date

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
    'OC' AS oc_position,
    1 AS opp_creation_weight,
    .3 AS w_shaped_weight,
    .225 AS full_path_weight
FROM opp_creation_base
WHERE touchpoint_number = 1