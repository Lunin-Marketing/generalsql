

  create  table "acton"."dbt_actonmarketing"."combined_attribution3__dbt_tmp"
  as (
    

WITH ao_combined AS (

   SELECT *
   FROM "acton"."dbt_actonmarketing"."ao_combined" 
   WHERE action_day >= '2022-09-15'

), lead_creation AS (

    SELECT *
    FROM "acton"."dbt_actonmarketing"."lead_creation2"
    WHERE action_day >= '2022-09-15'

), first_touch AS (

    SELECT *
    FROM "acton"."dbt_actonmarketing"."first_touch2"
    WHERE action_day >= '2022-09-15'

), last_touch AS (

    SELECT *
    FROM "acton"."dbt_actonmarketing"."last_touch"
    WHERE action_day >= '2022-09-15'

), opportunity_creation AS (

    SELECT *
    FROM "acton"."dbt_actonmarketing"."opportunity_creation2"
    WHERE action_day >= '2022-09-15'

), linear_base AS (

    SELECT
        email,
        COUNT(DISTINCT touchpoint_id)::Decimal AS linear_touches
    FROM ao_combined
    GROUP BY 1
        
), linear_final AS (

    SELECT
        ao_combined.email,
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
        1/linear_touches AS linear_weight
    FROM ao_combined
    LEFT JOIN linear_base ON 
    ao_combined.email=linear_base.email

), unioned AS (

    SELECT
        touchpoint_id,
        0 AS first_touch_weight,
        0 AS lead_creation_weight,
        opp_creation_weight,
        0 AS u_shaped_weight,
        w_shaped_weight,
        full_path_weight
    FROM opportunity_creation
    UNION ALL
    SELECT
        touchpoint_id,
        0 AS first_touch_weight,
        lead_creation_weight,
        0 AS opp_creation_weight,
        u_shaped_weight,
        w_shaped_weight,
        full_path_weight
    FROM lead_creation
    UNION ALL
    SELECT
        touchpoint_id,
        first_touch_weight,
        0 AS lead_creation_weight,
        0 AS opp_creation_weight,
        u_shaped_weight,
        w_shaped_weight,
        full_path_weight
    FROM first_touch

), unioned_final AS (

    SELECT
        touchpoint_id,
        SUM(first_touch_weight) AS first_touch_weight,
        SUM(lead_creation_weight) AS lead_creation_weight,
        SUM(opp_creation_weight) AS opp_creation_weight,
        SUM(u_shaped_weight) AS u_shaped_weight,
        SUM(w_shaped_weight) AS w_shaped_weight,
        SUM(full_path_weight) AS full_path_weight
    FROM unioned
    GROUP BY 1

), final_prep AS (

    SELECT
        linear_final.touchpoint_id,
        first_touch_weight,
        lead_creation_weight,
        opp_creation_weight,
        u_shaped_weight,
        w_shaped_weight,
        full_path_weight,
        linear_weight
    FROM linear_final
    LEFT JOIN unioned_final ON
    unioned_final.touchpoint_id=linear_final.touchpoint_id

), final AS (

    SELECT
    -- Key IDs
        lead_to_cw_cohort.email,
        ao_combined.touchpoint_id,
        opportunity_id,
        person_id,
        person_owner_id,

    -- Dates
        person_created_date,
        marketing_created_date,
        mql_created_date,
        sal_created_date,
        opp_created_date,
        discovery_date,
        demo_date,
        voc_date,
        closing_date,
        ao_combined.action_time,
        ao_combined.action_day,

    --Person Information
        lead_to_cw_cohort.company_size_rev,
        lead_to_cw_cohort.global_region,
        lead_to_cw_cohort.segment,
        lead_to_cw_cohort.channel_bucket,
        lead_to_cw_cohort.industry,
        lead_to_cw_cohort.industry_bucket,
        lead_to_cw_cohort.lead_source,
        lead_to_cw_cohort.channel_lead_creation,
        lead_to_cw_cohort.medium_lead_creation,
        lead_to_cw_cohort.source_lead_creation,
        lead_to_cw_cohort.person_offer_asset_name_lead_creation,

    --Opportunity Information
        lead_to_cw_cohort.acv,
        lead_to_cw_cohort.stage_name,
        lead_to_cw_cohort.opp_lead_source,
        lead_to_cw_cohort.opp_segment,
        lead_to_cw_cohort.account_global_region,
        lead_to_cw_cohort.opp_company_size_rev,
        lead_to_cw_cohort.opp_industry,
        lead_to_cw_cohort.opp_industry_bucket,
        lead_to_cw_cohort.opp_channel_bucket,
        lead_to_cw_cohort.opp_offer_asset_name_lead_creation,
        lead_to_cw_cohort.opp_type,

    --Flags
        lead_to_cw_cohort.is_hand_raiser,
        lead_to_cw_cohort.is_current_customer,

    --Touchpoint Information
        ao_combined.action,
        ao_combined.asset_id,
        ao_combined.asset_title,
        ao_combined.subject_line,
        ao_combined.from_address,
        ao_combined.clicked_url,
        ao_combined.clickthrough_link_name,
        ao_combined.referral_url,
        ao_combined.event_id,
        ao_combined.asset_type,
        CONCAT_WS(',',first_touch.ft_position,lead_creation.lc_position,opportunity_creation.oc_position,last_touch.lt_position) AS touchpoint_position,

    --Touchpoint Weights
        final_prep.first_touch_weight,
        final_prep.lead_creation_weight,
        final_prep.opp_creation_weight,
        final_prep.u_shaped_weight,
        final_prep.w_shaped_weight,
        final_prep.full_path_weight,
        final_prep.linear_weight,

    --Touchpoint ACV Weights
        lead_to_cw_cohort.acv*final_prep.first_touch_weight AS first_touch_acv,
        lead_to_cw_cohort.acv*final_prep.lead_creation_weight AS lead_creation_acv,
        lead_to_cw_cohort.acv*final_prep.opp_creation_weight AS opp_creation_acv,
        lead_to_cw_cohort.acv*final_prep.u_shaped_weight AS u_shaped_acv,
        lead_to_cw_cohort.acv*final_prep.w_shaped_weight AS w_shaped_acv,
        lead_to_cw_cohort.acv*final_prep.full_path_weight AS full_path_acv,
        lead_to_cw_cohort.acv*final_prep.linear_weight AS linear_acv
    FROM ao_combined
    LEFT JOIN final_prep ON
    ao_combined.touchpoint_id=final_prep.touchpoint_id
    LEFT JOIN "acton"."dbt_actonmarketing"."lead_to_cw_cohort2" lead_to_cw_cohort ON
    ao_combined.email=lead_to_cw_cohort.email
    LEFT JOIN lead_creation ON 
    ao_combined.touchpoint_id=lead_creation.touchpoint_id
    LEFT JOIN opportunity_creation ON 
    ao_combined.touchpoint_id=opportunity_creation.touchpoint_id
    LEFT JOIN first_touch ON 
    ao_combined.touchpoint_id=first_touch.touchpoint_id
    LEFT JOIN last_touch ON 
    ao_combined.touchpoint_id=last_touch.touchpoint_id

)

SELECT *
FROM final
  );