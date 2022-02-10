{{ config(materialized='table') }}
WITH base AS (
SELECT *
FROM "acton".salesforce."contact"

), final AS (
SELECT 
id AS contact_id,
is_deleted,
account_id,
first_name,
mailing_postal_code,
mailing_country,
email,
title,
lead_source,
DATE_TRUNC('day',created_date)::Date AS created_date,
last_modified_date,
system_modstamp AS systemmodstamp,
current_customer_reference_c AS is_current_customer,
no_longer_with_company_c AS is_no_longer_with_company,
hand_raiser_c AS is_hand_raiser,
mql_created_date_c AS mql_created_date,
mql_most_recent_date_c AS mql_most_recent_date,
contact_role_c AS contact_role,
primary_contact_c AS is_primary_contact,
ft_offerAsset_type_c AS offer_asset_type_first_touch,
ft_offer_asset_subtype_c AS offer_asset_subtype_first_touch,
contact_status_c AS contact_status,
ft_utm_channel_c AS channel_first_touch,
lt_utm_channel_c AS channel_last_touch,
lt_utm_medium_c AS medium_last_touch,
lt_utm_source_c AS source_last_touch,
lt_utm_campaign_c AS campaign_last_touch,
channel_lead_creation_c AS channel_lead_creation,
content_lead_creation_c AS content_lead_creation,
campaign_lead_creation_c AS campaign_lead_creation,
ft_offer_asset_topic_c AS offer_asset_topic_first_touch,
ft_offer_asset_name_c AS offer_asset_name_first_touch,
lc_offer_asset_type_c AS offer_asset_type_lead_creation,
lc_offer_asset_subtype_c AS offer_asset_subtype_lead_creation,
lc_offer_asset_topic_c AS offer_asset_topic_lead_creation,
lc_offer_asset_name_c AS offer_asset_name_lead_creation,
lt_offer_asset_type_c AS offer_asset_type_last_touch,
lt_offer_asset_subtype_c AS offer_asset_subtype_last_touch,
lt_offer_asset_name_c AS offer_asset_name_last_touch,
lt_offer_asset_topic_c AS offer_asset_topic_last_touch,
renewal_contact_c AS is_renewal_contact, --verify data type
account_owner_email_c AS account_owner_email,
account_csm_email_c AS account_csm_email,
ft_subchannel_c AS subchannel_first_touch,
lt_subchannel_c AS subchannel_last_touch,
lc_subchannel_c AS subchannel_lead_creation,
x_9883_lead_score_c AS lead_score,
status_reason_c AS status_reason,
marketing_lead_creation_date_c AS marketing_created_date,
current_map_c AS current_ma,
account_lookup_c AS account_lookup,
ft_utm_campaign_c AS campaign_first_touch,
ft_utm_medium_c AS medium_first_touch,
ft_utm_source_c AS source_first_touch,
-- AS mailing_postal_code,
-- AS mailing_country,
-- AS last_name, 
-- AS owner_id,
-- "Medium_Lead_Creation__c" AS medium_lead_creation,
-- "Source_Lead_Creation__c" AS source_lead_creation,
-- "Account_Owner_ID__c" AS account_owner_id, 
-- "Account_Owner__c" AS account_owner, --join to account
-- "Engagement_Level__c" AS engagement_level,
-- "Firmographic_Demographic_Lead_Grade__c" AS firmographic_demographic_lead_grade,
-- "Firmographic_Demographic_Lead_Score__c" AS firmographic_demographic_lead_score,
-- "Was_a_Handraiser_Lead__c" AS was_a_handraiser_lead,
-- "LeadID_Converted_From__c" AS lead_id_converted_from,
-- "Account_CSM__c" AS account_csm,-- join to account
-- "Account_CSM_ID__c" AS account_csm_id,-- join to account
-- "Account_SDR_Photo__c" AS account_sdr_photo,-- join to account
-- "Account_SDR_Phone__c" AS account_sdr_phone,-- join to account
-- "Account_SDR_Calendly__c" AS account_sdr_calendly,-- join to account
-- "Account_SDR_Title__c" AS account_sdr_title,-- join to account
-- "Account_Deliverability_Consultant_Email__c" AS account_deliverability_consultant_email,-- join to account
-- "Account_Deliverability_Consultant__c" AS account_deliverability_consultant,-- join to account
-- "Account_SDR_Full_Name__c" AS account_sdr_full_name,-- join to account
-- "Account_SDR_Email__c" AS account_sdr_email,-- join to account
-- "Owner_Email__c" AS owner_email, --join to user
-- annual_revenue, --join to account
CASE WHEN annual_revenue <= 49999999 THEN 'SMB'
     WHEN annual_revenue > 49999999 AND annual_revenue <= 499999999 THEN 'Mid-Market'
     WHEN annual_revenue > 499999999 THEN 'Enterprise'
     END AS company_size_rev,
CASE 
    WHEN LOWER(channel_lead_creation_c) = 'organic' THEN 'Organic'
    WHEN LOWER(channel_lead_creation_c) IS null THEN 'Unknown'
    WHEN LOWER(channel_lead_creation_c) = 'social' AND LOWER("Medium_Lead_Creation__c") = 'social-organic' THEN 'Social - Organic'
    WHEN LOWER(channel_lead_creation_c) = 'social' AND LOWER("Medium_Lead_Creation__c") = 'social-paid' THEN 'Paid Social'
    WHEN LOWER(channel_lead_creation_c) = 'ppc' THEN 'PPC/Paid Search'
    WHEN LOWER(channel_lead_creation_c) = 'email' AND LOWER("Source_Lead_Creation__c") like '%act-on%' THEN 'Paid Email' 
    WHEN LOWER(channel_lead_creation_c) = 'ppl' AND LOWER("Medium_Lead_Creation__c") = 'syndication partner' THEN 'PPL'
    WHEN LOWER(channel_lead_creation_c) IN ('prospecting','ppl') AND LOWER("Medium_Lead_Creation__c") = 'intent partner' THEN 'Intent Partners'
    WHEN LOWER(channel_lead_creation_c) = 'event' THEN 'Events and Trade Shows'
    WHEN LOWER(channel_lead_creation_c) = 'partner' THEN 'Partners'
    ELSE 'Other'
    END AS channel_bucket,
-- de_current_crm, --join to account
-- de_current_ma, --join to account
-- sdr
FROM base
LEFT JOIN "acton".dbt_actonmarketing.account_source_xf ON
base."AccountId"=account_source_xf.account_id
)

SELECT
*
FROM final