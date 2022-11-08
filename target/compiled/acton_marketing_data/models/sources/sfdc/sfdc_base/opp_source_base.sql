

WITH base AS (
SELECT *
FROM "acton"."salesforce"."opportunity"

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
        channel_lead_creation_c AS opp_channel_lead_creation,
        medium_lead_creation_c AS opp_medium_lead_creation,
        DATE_TRUNC('day',discovery_date_c)::Date AS discovery_date,
        DATE_TRUNC('day',date_reached_confirmed_value_c)::Date AS confirmed_value_date,
        DATE_TRUNC('day',date_reached_contract_c)::Date AS negotiation_date,
        DATE_TRUNC('day',date_reached_demo_c)::Date AS demo_date,
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
        DATE_TRUNC('day',sql_date_c)::Date AS sql_day,
        DATE_TRUNC('day',discovery_call_scheduled_date_time_c)::Date AS discovery_call_scheduled_datetime,
        DATE_TRUNC('day',discovery_call_completed_date_time_c)::Date AS discovery_call_completed_datetime,
        ao_account_id_c AS ao_account_id,
        lead_id_converted_from_c AS lead_id_converted_from,
        close_date,
        opportunity_type_detail_c AS opp_type_details,
        DATE_TRUNC('day',close_date)::Date AS close_day,
        source_lead_creation_c AS opp_source_lead_creation,
        oc_utm_campaign_c AS opp_campaign_opportunity_creation,
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
            WHEN LOWER(opp_channel_lead_creation) IS null AND opp_medium_lead_creation IS null AND opp_source_lead_creation IS null THEN 'Unknown'
            WHEN LOWER(opp_channel_lead_creation) = 'social' AND LOWER(opp_medium_lead_creation) = 'social-organic' THEN 'Social - Organic'
            WHEN LOWER(opp_channel_lead_creation) = 'social' AND LOWER(opp_medium_lead_creation) = 'social_organic' THEN 'Social - Organic'
            WHEN LOWER(opp_channel_lead_creation) = 'social' AND LOWER(opp_medium_lead_creation) = 'social-paid' THEN 'Paid Social'
            WHEN LOWER(opp_channel_lead_creation) = 'ppc' THEN 'PPC/Paid Search'
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_source_lead_creation) like '%act-on%' THEN 'Paid Email' 
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_medium_lead_creation) = 'email_paid' THEN 'Paid Email'
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_medium_lead_creation) = 'email-paid' THEN 'Paid Email' 
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_medium_lead_creation) = 'email_inhouse' THEN 'Paid Email' 
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_medium_lead_creation) = 'paid-email' THEN 'Paid Email' 
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_medium_lead_creation) = 'email' THEN 'Paid Email' 
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_medium_lead_creation) = 'syndication_partner' THEN 'Paid Email' 
            WHEN LOWER(opp_channel_lead_creation) = 'ppl' AND LOWER(opp_medium_lead_creation) = 'syndication partner' THEN 'PPL'  
            WHEN LOWER(opp_channel_lead_creation) IN ('prospecting','ppl') AND LOWER(opp_medium_lead_creation) = 'intent partner' THEN 'Intent Partners'
            WHEN LOWER(opp_channel_lead_creation) = 'ppl' THEN 'PPL'
            WHEN LOWER(opp_channel_lead_creation) = 'event' THEN 'Events and Trade Shows'
            WHEN LOWER(opp_channel_lead_creation) = 'partner' THEN 'Partners'
            WHEN LOWER(opp_medium_lead_creation) = 'virtualevent' THEN 'Events and Trade Shows'
            WHEN LOWER(opp_channel_lead_creation) = 'prospecting' AND LOWER(opp_medium_lead_creation) = 'sdr' THEN 'SDR'
            WHEN LOWER(opp_channel_lead_creation) = 'prospecting' AND LOWER(opp_medium_lead_creation) = 'rsm' THEN 'RSM'
            ELSE 'Other'
        END AS channel_bucket
    FROM intermediate
)

SELECT 
final.*
FROM final