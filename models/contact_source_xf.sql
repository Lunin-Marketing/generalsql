{{ config(materialized='table') }}

WITH base AS (

SELECT *
FROM "acton".salesforce."contact"

), final AS (

    SELECT 
        base.id AS contact_id,
        base.is_deleted,
        base.account_id,
        account_source_xf.account_name,
        base.first_name,
        base.mailing_postal_code,
        base.mailing_country,
        base.email,
        base.title,
        base.lead_source,
        DATE_TRUNC('day',base.created_date)::Date AS created_date,
        base.last_modified_date,
        base.system_modstamp AS systemmodstamp,
        base.current_customer_reference_c AS is_current_customer,
        base.no_longer_with_company_c AS is_no_longer_with_company,
        base.hand_raiser_c AS is_hand_raiser,
        base.mql_created_date_c AS mql_created_date,
        base.mql_most_recent_date_c AS mql_most_recent_date,
        base.contact_role_c AS contact_role,
        base.primary_contact_c AS is_primary_contact,
        base.ft_offer_asset_type_c AS offer_asset_type_first_touch,
        base.ft_offer_asset_subtype_c AS offer_asset_subtype_first_touch,
        base.contact_status_c AS contact_status,
        base.ft_utm_channel_c AS channel_first_touch,
        base.lt_utm_channel_c AS channel_last_touch,
        base.lt_utm_medium_c AS medium_last_touch,
        base.lt_utm_source_c AS source_last_touch,
        base.lt_utm_campaign_c AS campaign_last_touch,
        base.channel_lead_creation_c AS channel_lead_creation,
        base.content_lead_creation_c AS content_lead_creation,
        base.campaign_lead_creation_c AS campaign_lead_creation,
        base.ft_offer_asset_topic_c AS offer_asset_topic_first_touch,
        base.ft_offer_asset_name_c AS offer_asset_name_first_touch,
        base.lc_offer_asset_type_c AS offer_asset_type_lead_creation,
        base.lc_offer_asset_subtype_c AS offer_asset_subtype_lead_creation,
        base.lc_offer_asset_topic_c AS offer_asset_topic_lead_creation,
        base.lc_offer_asset_name_c AS offer_asset_name_lead_creation,
        base.lt_offer_asset_type_c AS offer_asset_type_last_touch,
        base.lt_offer_asset_subtype_c AS offer_asset_subtype_last_touch,
        base.lt_offer_asset_name_c AS offer_asset_name_last_touch,
        base.lt_offer_asset_topic_c AS offer_asset_topic_last_touch,
        base.renewal_contact_c AS is_renewal_contact, --verify data type
        base.account_owner_email_c AS account_owner_email,
        base.account_csm_email_c AS account_csm_email,
        base.ft_subchannel_c AS subchannel_first_touch,
        base.lt_subchannel_c AS subchannel_last_touch,
        base.lc_subchannel_c AS subchannel_lead_creation,
        base.x_9883_lead_score_c AS lead_score,
        base.status_reason_c AS status_reason,
        DATE_TRUNC('day',marketing_lead_creation_date_c)::Date AS marketing_created_date,
        base.current_map_c AS current_ma,
        base.account_lookup_c AS account_lookup,
        base.ft_utm_campaign_c AS campaign_first_touch,
        base.ft_utm_medium_c AS medium_first_touch,
        base.ft_utm_source_c AS source_first_touch,
        base.lead_id_converted_from_c AS lead_id_converted_from,
        base.was_a_handraiser_lead_c AS was_a_handraiser_lead,
        base.medium_lead_creation_c AS medium_lead_creation,
        base.source_lead_creation_c AS source_lead_creation,
        base.owner_id AS contact_owner_id,
        base.firmographic_demographic_lead_score_c AS firmographic_demographic_lead_score,
        base.last_name, 
        base.data_enrich_company_name_c AS de_account_name,
        DATE_TRUNC('day',date_time_to_working_c)::Date AS working_date,
        account_source_xf.account_owner_id,
        account_source_xf.account_owner_name,
        account_source_xf.annual_revenue,
        account_source_xf.de_current_crm,
        account_source_xf.de_current_ma,
        account_source_xf.account_csm_name AS account_csm_name,
        account_source_xf.csm_id AS account_csm_id,
        account_source_xf.sdr_phone AS account_sdr_phone,
        account_source_xf.sdr_photo AS account_sdr_photo,
        account_source_xf.sdr_calendly AS account_sdr_calendly,
        account_source_xf.sdr_title AS account_sdr_title,
        account_source_xf.sdr_full_name AS account_sdr_full_name,
        account_source_xf.sdr_email AS account_sdr_email,
        account_source_xf.account_deliverability_consultant_email,
        account_source_xf.account_deliverability_consultant,
        user_source_xf.user_email AS owner_email,
        CASE
            WHEN account_source_xf.annual_revenue <= 49999999 THEN 'SMB'
            WHEN account_source_xf.annual_revenue > 49999999 AND account_source_xf.annual_revenue <= 499999999 THEN 'Mid-Market'
            WHEN account_source_xf.annual_revenue > 499999999 THEN 'Enterprise'
        END AS company_size_rev,
        account_source_xf.global_region,
        account_source_xf.segment,
        account_source_xf.industry,
        CASE 
            WHEN LOWER(channel_lead_creation_c) = 'organic' THEN 'Organic'
            WHEN LOWER(channel_lead_creation_c) IS null THEN 'Unknown'
            WHEN LOWER(channel_lead_creation_c) = 'social' AND LOWER(medium_lead_creation_c) = 'social-organic' THEN 'Social - Organic'
            WHEN LOWER(channel_lead_creation_c) = 'social' AND LOWER(medium_lead_creation_c) = 'social-paid' THEN 'Paid Social'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' THEN 'PPC/Paid Search'
            WHEN LOWER(channel_lead_creation_c) = 'email' AND LOWER(source_lead_creation_c) like '%act-on%' THEN 'Paid Email' 
            WHEN LOWER(channel_lead_creation_c) = 'ppl' AND LOWER(medium_lead_creation_c) = 'syndication partner' THEN 'PPL'
            WHEN LOWER(channel_lead_creation_c) IN ('prospecting','ppl') AND LOWER(medium_lead_creation_c) = 'intent partner' THEN 'Intent Partners'
            WHEN LOWER(channel_lead_creation_c) = 'event' THEN 'Events and Trade Shows'
            WHEN LOWER(channel_lead_creation_c) = 'partner' THEN 'Partners'
            ELSE 'Other'
        END AS channel_bucket
    FROM base
    LEFT JOIN {{ref('account_source_xf')}} ON
    base.account_id=account_source_xf.account_id
    LEFT JOIN {{ref('user_source_xf')}} ON
    base.owner_id=user_source_xf.user_id

)

SELECT *
FROM final