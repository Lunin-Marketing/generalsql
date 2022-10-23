

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
    LEFT JOIN "acton"."dbt_actonmarketing"."lead_to_cw_cohort" cohort ON
    base.email=cohort.email
    WHERE action_day <= cohort.opp_created_date

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
        WHEN touchpoint_number = 1 THEN 'OC' 
        ELSE null
    END AS oc_position,
    CASE
        WHEN touchpoint_number = 1 THEN 1
        ELSE 0
    END AS opp_creation_weight,
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
LEFT JOIN opp_creation_base ON
base.touchpoint_id=opp_creation_base.touchpoint_id
LEFT JOIN linear_base ON 
base.email=linear_base.email
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18