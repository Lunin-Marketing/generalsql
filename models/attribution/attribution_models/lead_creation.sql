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
    LEFT JOIN {{ref('person_source_xf')}} person ON
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
    'Lead Creation' AS touchpoint_position,
    '1' AS lead_creation_weight
FROM lead_creation_base
WHERE touchpoint_number = 1
AND email = '100443596@alumnos.uc3m.es'