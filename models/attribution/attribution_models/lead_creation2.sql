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

), person_source_prep AS (

    SELECT
        email,
        {{ dbt_date.convert_timezone("created_date::Timestamp", "America/Los_Angeles") }} AS created_date
    FROM {{ref('person_source_xf')}}

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
    AND asset_type = 'form'

), linear_base AS (

    SELECT
        email,
        COUNT(DISTINCT touchpoint_id) AS linear_touches
    FROM base
    GROUP BY 1
)

SELECT
    base.touchpoint_id,
    base.action,
    base.action_time,
    base.action_day,
    base.asset_id,
    base.email,
    base.asset_title,
    base.subject_line,
    base.from_address,
    base.clicked_url,
    base.clickthrough_link_name,
    base.referral_url,
    base.event_id,
    base.asset_type,
    touchpoint_number,
    linear_touches,
    CASE
        WHEN touchpoint_number = 1 THEN 'LC' 
        ELSE null
    END AS lc_position,
    CASE
        WHEN touchpoint_number = 1 THEN 1
        ELSE 0
    END AS lead_creation_weight,
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
FROM base
LEFT JOIN lead_creation_base ON
base.touchpoint_id=lead_creation_base.touchpoint_id
LEFT JOIN linear_base ON
base.email=linear_base.email
{{ dbt_utils.group_by(n=19) }}