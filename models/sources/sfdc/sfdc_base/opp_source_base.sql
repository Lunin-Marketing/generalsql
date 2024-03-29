{{ config(materialized='table') }}

WITH base AS (
SELECT *
FROM {{ source('salesforce', 'opportunity') }}

), intermediate AS (

    SELECT
        id AS opportunity_id,
        is_deleted,
        account_id,
        name AS opportunity_name,
        stage_name,
        amount,
        type,
        lead_source AS opp_lead_source,
        is_closed,
        is_won,
        owner_id,
        created_date AS created_day,
        DATE_TRUNC('day',created_date)::Date AS created_date,
        DATE_TRUNC('day',last_modified_date)::Date AS last_modified_date,
        system_modstamp AS systemmodstamp,
        contact_id,
        contract_id,
        crm_c AS opp_crm,
        renewal_type_c AS renewal_type,
        renewal_acv_value_c AS renewal_acv,
        COALESCE(oc_utm_channel_c,channel_lead_creation_c) AS opp_channel_lead_creation,
        COALESCE(oc_utm_medium_c,medium_lead_creation_c) AS opp_medium_lead_creation,
        COALESCE(oc_utm_source_c,source_lead_creation_c) AS opp_source_lead_creation,
        DATE_TRUNC('day',discovery_date_c)::Date AS discovery_date,
        DATE_TRUNC('day',date_reached_confirmed_value_c)::Date AS confirmed_value_date,
        DATE_TRUNC('day',date_reached_contract_c)::Date AS negotiation_date,
        DATE_TRUNC('day',date_reached_demo_c)::Date AS date_reached_demo,
        DATE_TRUNC('day',date_reached_demo_confirmed_c)::Date AS date_reached_demo_confirmed,
        DATE_TRUNC('day',date_reached_demo_complete_c)::Date AS date_reached_demo_complete,
        DATE_TRUNC('day',date_reached_solution_c)::Date AS solution_date,
        DATE_TRUNC('day',date_reached_closing_c)::Date AS closing_date,
        DATE_TRUNC('day',date_time_reached_implement_c)::Date AS implement_date,
        DATE_TRUNC('day',sql_date_c)::Date AS sql_date,
        DATE_TRUNC('day',date_time_reached_voc_negotiate_c)::Date AS voc_date,
        DATE_TRUNC('day',date_time_reached_discovery_c)::Date AS discovery_day_time,
        DATE_TRUNC('day',date_time_reached_demo_c)::Date AS demo_day_time,
        DATE_TRUNC('day',date_time_reached_implement_c)::Date AS implement_day_time,
        DATE_TRUNC('day',date_time_reached_sql_c)::Date AS sql_day_time,
        DATE_TRUNC('day',date_time_reached_voc_negotiate_c)::Date AS voc_day_time,
        oc_utm_channel_c AS opp_channel_opportunity_creation,
        oc_utm_medium_c AS opp_medium_opportunity_creation,
        oc_utm_content_c AS opp_content_opportunity_creation, 
        oc_utm_source_c AS opp_source_opportunity_creation, 
        csm_c AS csm,
        marketing_channel_c AS marketing_channel,
        ft_utm_channel_c AS opp_channel_first_touch,
        ft_utm_content_c AS opp_content_first_touch,
        ft_utm_medium_c AS opp_medium_first_touch,
        ft_utm_source_c AS opp_source_first_touch,
        -- lt_utm_channel_c AS opp_channel_last_touch,
        -- lt_utm_content_c AS opp_content_last_touch,
        -- lt_utm_medium_c AS opp_medium_last_touch,
        -- lt_utm_source_c AS opp_source_last_touch,
        oc_offer_asset_type_c AS opp_offer_asset_type_opportunity_creation,
        oc_offer_asset_subtype_c AS opp_offer_asset_subtype_opportunity_creation,
        oc_offer_asset_topic_c AS opp_offer_asset_topic_opportunity_creation,
        oc_offer_asset_name_c AS opp_offer_asset_name_opportunity_creation,
        ft_offer_asset_name_c AS opp_offer_asset_name_first_touch,
        lc_offer_asset_name_c  AS opp_offer_asset_name_lead_creation, 
        ft_offer_asset_subtype_c AS opp_offer_asset_subtype_first_touch, 
        lc_offer_asset_subtype_c AS opp_offer_asset_subtype_lead_creation,
        ft_offer_asset_topic_c AS opp_offer_asset_topic_first_touch, 
        lc_offer_asset_topic_c AS opp_offer_asset_topic_lead_creation,
        ft_offer_asset_type_c AS opp_offer_asset_type_first_touch, 
        lc_offer_asset_type_c AS opp_offer_asset_type_lead_creation,
        ft_subchannel_c AS opp_subchannel_first_touch,
        lc_subchannel_c AS opp_subchannel_lead_creation, 
        oc_subchannel_c AS opp_subchannel_opportunity_creation,
        DATE_TRUNC('day',discovery_call_scheduled_date_c)::Date AS discovery_call_date,
        opportunity_status_c AS opportunity_status,
        sql_status_reason_c AS sql_status_reason,
        reason_c AS closed_lost_reason, 
        DATE_TRUNC('day',sql_date_c)::Date AS sql_day,
        DATE_TRUNC('day',discovery_call_scheduled_date_time_c)::Date AS discovery_call_scheduled_datetime,
        DATE_TRUNC('day',discovery_call_completed_date_time_c)::Date AS discovery_call_completed_datetime,
        ao_account_id_c AS ao_account_id,
        lead_id_converted_from_c AS lead_id_converted_from,
        close_date,
        opportunity_type_detail_c AS opp_type_details,
        DATE_TRUNC('day',close_date)::Date AS close_day,
        oc_utm_campaign_c AS opp_campaign_opportunity_creation,
        campaign_id AS primary_campaign_id,
        forecast_category,
        ft_utm_campaign_c AS opp_campaign_first_touch,  
        acv_deal_size_override_c AS acv_deal_size_override,
        lead_grade_at_conversion_c AS lead_grade_at_conversion,
        renewal_stage_c AS renewal_stage,
        created_by_id,
        quota_credit_renewal_c AS quota_credit_renewal,
        sbqq_renewed_contract_c AS renewed_contract_id,
        quota_credit_c AS quota_credit,
        sbqq_primary_quote_c AS primary_quote_id,
        quota_credit_new_business_c AS quota_credit_new_business,
        quota_credit_one_time_c AS quota_credit_one_time,
        submitted_for_approval_c AS submitted_for_approval,
        acv_add_back_c AS acv_add_back,
        trigger_renewal_value_c AS trigger_renewal_value,
        acv_deal_size_usd_stamp_c AS acv_deal_size_usd
    FROM base
    WHERE base.is_deleted = 'False'

), final AS (

    SELECT
        intermediate.*,
        COALESCE(date_reached_demo,date_reached_demo_confirmed,date_reached_demo_complete) AS demo_date,
        CASE 
            WHEN acv_deal_size_usd <= '9999' THEN '< 10K'
            WHEN acv_deal_size_usd > '9999' AND acv_deal_size_usd <= '14999' THEN '10-15K'
            WHEN acv_deal_size_usd > '14999' AND acv_deal_size_usd <= '19999' THEN '15-20K'
            WHEN acv_deal_size_usd > '19999' AND acv_deal_size_usd <= '24999' THEN '20-25K'
            WHEN acv_deal_size_usd > '24999' AND acv_deal_size_usd <= '29999' THEN '25-30K'
            ELSE '30K+'
        END AS deal_size_range,
        CASE 
            WHEN LOWER(opp_channel_lead_creation) = 'organic' THEN 'Organic'
            WHEN LOWER(opp_channel_lead_creation) = 'direct mail' THEN 'Direct Mail'
            WHEN LOWER(opp_channel_lead_creation) = 'phone' THEN 'Phone'
            WHEN LOWER(opp_channel_lead_creation) IS null AND opp_medium_lead_creation IS null AND opp_source_lead_creation IS null THEN 'Unknown'
            WHEN LOWER(opp_channel_lead_creation) = 'product' AND LOWER(opp_medium_lead_creation) = 'product - login' THEN 'Product - Lead'
            WHEN LOWER(opp_channel_lead_creation) = 'social' AND LOWER(opp_medium_lead_creation) = 'social-organic' THEN 'Social - Organic'
            WHEN LOWER(opp_channel_lead_creation) = 'social' AND LOWER(opp_medium_lead_creation) = 'social_organic' THEN 'Social - Organic'
            WHEN LOWER(opp_channel_lead_creation) = 'social' AND LOWER(opp_medium_lead_creation) = 'social-paid' THEN 'Social - Paid'
            WHEN LOWER(opp_channel_lead_creation) IS null AND LOWER(opp_medium_lead_creation) = 'social-paid' THEN 'Social - Paid'
            WHEN LOWER(opp_channel_lead_creation) = 'social' AND LOWER(opp_medium_lead_creation) = 'paid-social' THEN 'Paid Social'
            WHEN LOWER(opp_channel_lead_creation) = 'ppc' AND LOWER(opp_medium_lead_creation) = 'cpc' THEN 'PPC - Display'
            WHEN LOWER(opp_channel_lead_creation) = 'ppc' AND LOWER(opp_medium_lead_creation) = 'display' THEN 'PPC - Display'
            WHEN LOWER(opp_channel_lead_creation) = 'ppc' AND LOWER(opp_medium_lead_creation) = 'intent partner' THEN 'PPC - Intent'
            WHEN LOWER(opp_channel_lead_creation) = 'ppc' AND LOWER(opp_medium_lead_creation) = 'search' THEN 'PPC - Paid Search'
            WHEN LOWER(opp_channel_lead_creation) = 'ppc' AND LOWER(opp_medium_lead_creation) = 'social-paid' THEN 'PPC - Social'
            WHEN LOWER(opp_channel_lead_creation) = 'ppc' AND LOWER(opp_medium_lead_creation) IS NOT null THEN 'PPC - Paid Search'
            WHEN LOWER(opp_channel_lead_creation) = 'ppc' AND LOWER(opp_medium_lead_creation) IS null THEN 'PPC - General'
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_source_lead_creation) like '%act-on%' THEN 'Email - Paid' 
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_medium_lead_creation) = 'email_paid' THEN 'Email - Paid'
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_medium_lead_creation) = 'email-paid' THEN 'Email - Paid'
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_medium_lead_creation) = 'email- paid' THEN 'Email - Paid' 
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_medium_lead_creation) = 'email_inhouse' THEN 'Email - Paid' 
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_medium_lead_creation) = 'paid-email' THEN 'Email - Paid' 
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_medium_lead_creation) = 'email' THEN 'Email - Paid' 
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_medium_lead_creation) = 'syndication_partner' THEN 'Email - Paid' 
            WHEN LOWER(opp_channel_lead_creation) = 'ppl' AND LOWER(opp_medium_lead_creation) = 'syndication partner' THEN 'PPL - Syndication'  
            WHEN LOWER(opp_channel_lead_creation) = 'ppl' AND LOWER(opp_medium_lead_creation) = 'intent partner' THEN 'PPL - Intent'
            WHEN LOWER(opp_channel_lead_creation) = 'ppl' THEN 'PPL'
            WHEN LOWER(opp_channel_lead_creation) = 'prospecting' AND LOWER(opp_medium_lead_creation) = 'intent partner' THEN 'Prospecting - Intent Partners'
            WHEN LOWER(opp_channel_lead_creation) = 'events' AND LOWER(opp_medium_lead_creation) = 'webinar' THEN 'Webinar'
            WHEN LOWER(opp_channel_lead_creation) IN ('event','events') THEN 'Events and Trade Shows'
            WHEN LOWER(opp_channel_lead_creation) = 'partner' THEN 'Partners'
            WHEN LOWER(opp_medium_lead_creation) = 'partner' THEN 'Partners'
            WHEN LOWER(opp_medium_lead_creation) = 'virtualevent' THEN 'Events and Trade Shows'
            WHEN LOWER(opp_channel_lead_creation) = 'prospecting' AND LOWER(opp_medium_lead_creation) = 'sdr' THEN 'Prospecting - SDR'
            WHEN LOWER(opp_channel_lead_creation) = 'prospecting' AND LOWER(opp_medium_lead_creation) = 'rsm' THEN 'Prospecting - RSM'
            WHEN LOWER(opp_channel_lead_creation) = 'prospecting' AND LOWER(opp_medium_lead_creation) = 'channel management' THEN 'Prospecting - Channel'
            WHEN LOWER(opp_channel_lead_creation) = 'prospecting' AND LOWER(opp_medium_lead_creation) = 'third-party' THEN 'Prospecting - Third Party'
            WHEN LOWER(opp_channel_lead_creation) = 'prospecting' THEN 'Prospecting - General'
            WHEN LOWER(opp_channel_lead_creation) = 'referral' THEN 'Referral - General'
            WHEN LOWER(opp_channel_lead_creation) = 'predates attribution' AND LOWER(opp_medium_lead_creation) = 'predates attribution' THEN 'Predates Attribution'
            ELSE 'Other'
        END AS channel_bucket_details,
        CASE 
            WHEN LOWER(opp_channel_lead_creation) = 'organic' THEN 'Organic'
            WHEN LOWER(opp_channel_lead_creation) = 'direct mail' THEN 'Direct Mail'
            WHEN LOWER(opp_channel_lead_creation) = 'phone' THEN 'Phone'
            WHEN LOWER(opp_channel_lead_creation) IS null AND opp_medium_lead_creation IS null AND opp_source_lead_creation IS null THEN 'Unknown'
            WHEN LOWER(opp_channel_lead_creation) = 'product' AND LOWER(opp_medium_lead_creation) = 'product - login' THEN 'Product'
            WHEN LOWER(opp_channel_lead_creation) = 'social' AND LOWER(opp_medium_lead_creation) = 'social-organic' THEN 'Social'
            WHEN LOWER(opp_channel_lead_creation) = 'social' AND LOWER(opp_medium_lead_creation) = 'social_organic' THEN 'Social'
            WHEN LOWER(opp_channel_lead_creation) = 'social' AND LOWER(opp_medium_lead_creation) = 'social-paid' THEN 'Paid Social'
            WHEN LOWER(opp_channel_lead_creation) = 'social' AND LOWER(opp_medium_lead_creation) = 'paid-social' THEN 'Paid Social'
            WHEN LOWER(opp_channel_lead_creation) IS null AND LOWER(opp_medium_lead_creation) = 'social-paid' THEN 'Organic Social'
            WHEN LOWER(opp_channel_lead_creation) = 'ppc' AND LOWER(opp_medium_lead_creation) = 'cpc' THEN 'PPC'
            WHEN LOWER(opp_channel_lead_creation) = 'ppc' AND LOWER(opp_medium_lead_creation) = 'display' THEN 'PPC'
            WHEN LOWER(opp_channel_lead_creation) = 'ppc' AND LOWER(opp_medium_lead_creation) = 'intent partner' THEN 'PPC'
            WHEN LOWER(opp_channel_lead_creation) = 'ppc' AND LOWER(opp_medium_lead_creation) = 'search' THEN 'Paid Search'
            WHEN LOWER(opp_channel_lead_creation) = 'ppc' AND LOWER(opp_medium_lead_creation) = 'social-paid' THEN 'PPC'
            WHEN LOWER(opp_channel_lead_creation) = 'ppc' AND LOWER(opp_medium_lead_creation) IS NOT null THEN 'PPC'
            WHEN LOWER(opp_channel_lead_creation) = 'ppc' AND LOWER(opp_medium_lead_creation) IS null THEN 'PPC'
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_source_lead_creation) like '%act-on%' THEN 'Email' 
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_medium_lead_creation) = 'email_paid' THEN 'Email'
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_medium_lead_creation) = 'email-paid' THEN 'Email'
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_medium_lead_creation) = 'email- paid' THEN 'Email' 
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_medium_lead_creation) = 'email_inhouse' THEN 'Email' 
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_medium_lead_creation) = 'paid-email' THEN 'Email' 
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_medium_lead_creation) = 'email' THEN 'Email' 
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_medium_lead_creation) = 'syndication_partner' THEN 'Email' 
            WHEN LOWER(opp_channel_lead_creation) = 'ppl' AND LOWER(opp_medium_lead_creation) = 'syndication partner' THEN 'PPL'  
            WHEN LOWER(opp_channel_lead_creation) = 'ppl' AND LOWER(opp_medium_lead_creation) = 'intent partner' THEN 'PPL'
            WHEN LOWER(opp_channel_lead_creation) = 'ppl' THEN 'PPL'
            WHEN LOWER(opp_channel_lead_creation) = 'prospecting' AND LOWER(opp_medium_lead_creation) = 'intent partner' THEN 'Prospecting'
            WHEN LOWER(opp_channel_lead_creation) = 'events' AND LOWER(opp_medium_lead_creation) = 'webinar' THEN 'Webinar'
            WHEN LOWER(opp_channel_lead_creation) IN ('event','events') THEN 'Event'
            WHEN LOWER(opp_channel_lead_creation) = 'partner' THEN 'Partner'
            WHEN LOWER(opp_medium_lead_creation) = 'partner' THEN 'Partner'
            WHEN LOWER(opp_medium_lead_creation) = 'virtualevent' THEN 'Event'
            WHEN LOWER(opp_channel_lead_creation) = 'prospecting' AND LOWER(opp_medium_lead_creation) = 'sdr' THEN 'Prospecting'
            WHEN LOWER(opp_channel_lead_creation) = 'prospecting' AND LOWER(opp_medium_lead_creation) = 'rsm' THEN 'Prospecting'
            WHEN LOWER(opp_channel_lead_creation) = 'prospecting' AND LOWER(opp_medium_lead_creation) = 'channel management' THEN 'Prospecting'
            WHEN LOWER(opp_channel_lead_creation) = 'prospecting' AND LOWER(opp_medium_lead_creation) = 'third-party' THEN 'Prospecting'
            WHEN LOWER(opp_channel_lead_creation) = 'prospecting' THEN 'Prospecting'
            WHEN LOWER(opp_channel_lead_creation) = 'referral' THEN 'Partner_Referral'
            WHEN LOWER(opp_channel_lead_creation) = 'predates attribution' AND LOWER(opp_medium_lead_creation) = 'predates attribution' THEN 'Predates Attribution'
            ELSE 'Other'
        END AS channel_bucket--,
        -- CASE 
        --     WHEN LOWER(opp_channel_last_touch) = 'organic' THEN 'Organic'
        --     WHEN LOWER(opp_channel_last_touch) = 'direct mail' THEN 'Direct Mail'
        --     WHEN LOWER(opp_channel_last_touch) = 'phone' THEN 'Phone'
        --     WHEN LOWER(opp_channel_last_touch) IS null AND opp_medium_last_touch IS null AND opp_source_last_touch IS null THEN 'Unknown'
        --     WHEN LOWER(opp_channel_last_touch) = 'product' AND LOWER(opp_medium_last_touch) = 'product - login' THEN 'Product'
        --     WHEN LOWER(opp_channel_last_touch) = 'social' AND LOWER(opp_medium_last_touch) = 'social-organic' THEN 'Social'
        --     WHEN LOWER(opp_channel_last_touch) = 'social' AND LOWER(opp_medium_last_touch) = 'social_organic' THEN 'Social'
        --     WHEN LOWER(opp_channel_last_touch) = 'social' AND LOWER(opp_medium_last_touch) = 'social-paid' THEN 'Paid Social'
        --     WHEN LOWER(opp_channel_last_touch) = 'social' AND LOWER(opp_medium_last_touch) = 'paid-social' THEN 'Paid Social'
        --     WHEN LOWER(opp_channel_last_touch) IS null AND LOWER(opp_medium_last_touch) = 'social-paid' THEN 'Organic Social'
        --     WHEN LOWER(opp_channel_last_touch) = 'ppc' AND LOWER(opp_medium_last_touch) = 'cpc' THEN 'PPC'
        --     WHEN LOWER(opp_channel_last_touch) = 'ppc' AND LOWER(opp_medium_last_touch) = 'display' THEN 'PPC'
        --     WHEN LOWER(opp_channel_last_touch) = 'ppc' AND LOWER(opp_medium_last_touch) = 'intent partner' THEN 'PPC'
        --     WHEN LOWER(opp_channel_last_touch) = 'ppc' AND LOWER(opp_medium_last_touch) = 'search' THEN 'Paid Search'
        --     WHEN LOWER(opp_channel_last_touch) = 'ppc' AND LOWER(opp_medium_last_touch) = 'social-paid' THEN 'PPC'
        --     WHEN LOWER(opp_channel_last_touch) = 'ppc' AND LOWER(opp_medium_last_touch) IS NOT null THEN 'PPC'
        --     WHEN LOWER(opp_channel_last_touch) = 'ppc' AND LOWER(opp_medium_last_touch) IS null THEN 'PPC'
        --     WHEN LOWER(opp_channel_last_touch) = 'email' AND LOWER(opp_source_last_touch) like '%act-on%' THEN 'Email' 
        --     WHEN LOWER(opp_channel_last_touch) = 'email' AND LOWER(opp_medium_last_touch) = 'email_paid' THEN 'Email'
        --     WHEN LOWER(opp_channel_last_touch) = 'email' AND LOWER(opp_medium_last_touch) = 'email-paid' THEN 'Email'
        --     WHEN LOWER(opp_channel_last_touch) = 'email' AND LOWER(opp_medium_last_touch) = 'email- paid' THEN 'Email' 
        --     WHEN LOWER(opp_channel_last_touch) = 'email' AND LOWER(opp_medium_last_touch) = 'email_inhouse' THEN 'Email' 
        --     WHEN LOWER(opp_channel_last_touch) = 'email' AND LOWER(opp_medium_last_touch) = 'paid-email' THEN 'Email' 
        --     WHEN LOWER(opp_channel_last_touch) = 'email' AND LOWER(opp_medium_last_touch) = 'email' THEN 'Email' 
        --     WHEN LOWER(opp_channel_last_touch) = 'email' AND LOWER(opp_medium_last_touch) = 'syndication_partner' THEN 'Email' 
        --     WHEN LOWER(opp_channel_last_touch) = 'ppl' AND LOWER(opp_medium_last_touch) = 'syndication partner' THEN 'PPL'  
        --     WHEN LOWER(opp_channel_last_touch) = 'ppl' AND LOWER(opp_medium_last_touch) = 'intent partner' THEN 'PPL'
        --     WHEN LOWER(opp_channel_last_touch) = 'ppl' THEN 'PPL'
        --     WHEN LOWER(opp_channel_last_touch) = 'prospecting' AND LOWER(opp_medium_last_touch) = 'intent partner' THEN 'Prospecting'
        --     WHEN LOWER(opp_channel_last_touch) = 'events' AND LOWER(opp_medium_last_touch) = 'webinar' THEN 'Webinar'
        --     WHEN LOWER(opp_channel_last_touch) IN ('event','events') THEN 'Event'
        --     WHEN LOWER(opp_channel_last_touch) = 'partner' THEN 'Partner'
        --     WHEN LOWER(opp_medium_last_touch) = 'partner' THEN 'Partner'
        --     WHEN LOWER(opp_medium_last_touch) = 'virtualevent' THEN 'Event'
        --     WHEN LOWER(opp_channel_last_touch) = 'prospecting' AND LOWER(opp_medium_last_touch) = 'sdr' THEN 'Prospecting'
        --     WHEN LOWER(opp_channel_last_touch) = 'prospecting' AND LOWER(opp_medium_last_touch) = 'rsm' THEN 'Prospecting'
        --     WHEN LOWER(opp_channel_last_touch) = 'prospecting' AND LOWER(opp_medium_last_touch) = 'channel management' THEN 'Prospecting'
        --     WHEN LOWER(opp_channel_last_touch) = 'prospecting' AND LOWER(opp_medium_last_touch) = 'third-party' THEN 'Prospecting'
        --     WHEN LOWER(opp_channel_last_touch) = 'prospecting' THEN 'Prospecting'
        --     WHEN LOWER(opp_channel_last_touch) = 'referral' THEN 'Partner_Referral'
        --     WHEN LOWER(opp_channel_last_touch) = 'predates attribution' AND LOWER(opp_medium_last_touch) = 'predates attribution' THEN 'Predates Attribution'
        --     ELSE 'Other'
        -- END AS channel_bucket_lt
        FROM intermediate
)

SELECT 
final.*
FROM final