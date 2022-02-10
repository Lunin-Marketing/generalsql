{{ config(materialized='table') }}
WITH base AS (
SELECT *
FROM "acton".salesforce."ead"

), final AS (
SELECT
id AS lead_id,
is_deleted,
first_name,
last_name, 
title,
company,
state,
country,
email,
lead_source,
status AS lead_status,
industry,
annual_revenue,
number_of_employees,
owner_id AS lead_owner,
is_converted,
DATE_TRUNC('day',converted_date)::Date AS converted_date,
converted_account_id,
converted_contact_id,
converted_opportunity_id,
last_modified_date,
DATE_TRUNC('day',created_date)::Date AS created_date,
system_modstamp AS systemmodstamp,
DATE_TRUNC('day',mql_created_date)::Date AS mql_created_date,
DATE_TRUNC('day',mql_most_recent_date)::Date AS mql_most_recent_date,
account_c AS account_id,
no_longer_with_company_c AS no_longer_with_company,
ft_utm_channel_c AS channel_first_touch,
lt_utm_channel_c AS channel_last_touch,
lt_utm_medium_c AS medium_last_touch,
lt_utm_content_c AS content_last_touch,
lt_utm_source_c AS source_last_touch,
lt_utm_campaign_c AS campaign_last_touch,
channel_lead_creation_c AS channel_lead_creation,
medium_lead_creation_c AS medium_lead_creation,
hand_raiser_c AS is_hand_raiser,
ft_subchannel_c AS subchannel_first_touch,
lt_subchannel_c AS subchannel_last_touch,
lc_subchannel_c AS subchannel_lead_creation,
ft_offer_asset_type_c AS offer_asset_type_first_touch,
ft_offer_asset_subtype_c AS offer_asset_subtype_first_touch,
ft_offer_asset_topic_c AS offer_asset_topic_first_touch,
ft_offer_asset_name_c AS offer_asset_name_first_touch,
lc_offer_asset_type_c AS offer_asset_type_lead_creation,
lc_offer_asset_subtype_c AS offer_asset_subtype_lead_creation,
lc_offer_asset_topic_c AS offer_asset_topic_lead_creation,
lc_offer_asset_name_c AS offer_asset_name_lead_creation,
lt_offer_asset_type_c AS offer_asset_type_last_touch,
lt_offer_asset_subtype_c AS offer_asset_subtype_last_touch,
lt_offer_asset_topic_c AS offer_asset_topic_last_touch,
lt_offer_asset_name_c AS offer_asset_name_last_touch,
lean_data_a_2_b_account_c AS lean_data_account_id,
de_current_marketing_automation_c AS current_ma,
de_current_crm_c AS current_crm,
DATE_TRUNC('day',marketing_lead_creation_date_c)::Date AS marketing_created_date,
mql_created_time_c AS mql_created_datetime,
mql_most_recent_time_c AS mql_most_recent_datetime,
article_14_notice_date_c AS article_14_notice_date,
x_9883_lead_score_c AS lead_score,
ft_utm_medium_c AS medium_first_touch,
ft_utm_source_c AS source_first_touch,
ft_utm_campaign_c AS campaign_first_touch,
channel_lead_creation_c AS lead_channel_forecast,


"Source_Lead_Creation__c" AS source_lead_creation,
"EmailBouncedReason" AS email_bounced_reason,
"EmailBouncedDate" AS email_bounced_date,
"FirmographicDemographic_Lead_Score__c" AS firmographic_demographic_lead_score,
"Do_Not_Contact__c" AS do_not_contact,
"Form_Consent_Opt_In__c" AS form_consent_opt_in,
"Campaign_Lead_Creation__c" AS campaign_lead_creation,
"Legitimate_Basis__c" AS legitimate_basis,
CASE WHEN annual_revenue <= 49999999 THEN 'SMB'
     WHEN annual_revenue > 49999999 AND annual_revenue <= 499999999 THEN 'Mid-Market'
     WHEN annual_revenue > 499999999 THEN 'Enterprise'
     END AS company_size_rev,
CASE 
    WHEN LOWER(channel_lead_creation) = 'organic' THEN 'Organic'
    WHEN LOWER(channel_lead_creation) IS null THEN 'Unknown'
    WHEN LOWER(channel_lead_creation) = 'social' AND LOWER("Medium_Lead_Creation__c") = 'social-organic' THEN 'Social - Organic'
    WHEN LOWER(channel_lead_creation) = 'social' AND LOWER("Medium_Lead_Creation__c") = 'social-paid' THEN 'Paid Social'
    WHEN LOWER(channel_lead_creation) = 'ppc' THEN 'PPC/Paid Search'
    WHEN LOWER(channel_lead_creation) = 'email' AND LOWER("Source_Lead_Creation__c") like '%act-on%' THEN 'Paid Email' 
    WHEN LOWER(channel_lead_creation) = 'ppl' AND LOWER("Medium_Lead_Creation__c") = 'syndication partner' THEN 'PPL'
    WHEN LOWER(channel_lead_creation) IN ('prospecting','ppl') AND LOWER("Medium_Lead_Creation__c") = 'intent partner' THEN 'Intent Partners'
    WHEN LOWER(channel_lead_creation) = 'event' THEN 'Events and Trade Shows'
    WHEN LOWER(channel_lead_creation) = 'partner' THEN 'Partners'
    ELSE 'Other'
    END AS channel_bucket,
COALESCE(account_id,lean_data_account_id) AS person_account_id
FROM base
WHERE base."OwnerId" != '00Ga0000003Nugr' -- AO-Fake Leads
)

SELECT 
*
FROM final