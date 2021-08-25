{{ config(materialized='table') }}
WITH base AS (
SELECT *
FROM "acton".public."Lead"

), final AS (
SELECT
"Id" AS lead_id,
"IsDeleted" AS is_deleted,
"FirstName" AS first_name,
"LastName" AS last_name, 
"Title" AS title,
"Company" AS company,
"State" AS state,
"Country" AS country,
"Email" AS email,
"Status" AS lead_status,
"Industry" AS industry,
"LeadSource" AS lead_source,
"AnnualRevenue" AS annual_revenue,
"OwnerId" AS lead_owner,
"NumberOfEmployees" AS number_of_employees,
"IsConverted" AS is_converted,
DATE_TRUNC('day',"ConvertedDate")::Date AS converted_date,
"ConvertedAccountId" AS converted_account_id,
"ConvertedContactId" AS converted_contact_id,
"ConvertedOpportunityId" AS converted_opportunity_id,
"LastModifiedDate" AS last_modified_date,
DATE_TRUNC('day',"Marketing_Lead_Creation_Date__c")::Date AS created_date,
"EmailBouncedReason" AS email_bounced_reason,
"EmailBouncedDate" AS email_bounced_date,
DATE_TRUNC('day',"MQL_Created_Date__c")::Date AS mql_created_date,
DATE_TRUNC('day',"MQL_Most_Recent_Date__c")::Date AS mql_most_recent_date,
"Firmographic_Demographic_Lead_Grade__c" AS firmographic_demographic_lead_grade,
"FirmographicDemographic_Lead_Score__c" AS firmographic_demographic_lead_score,
"No_Longer_with_Company__c" AS no_longer_with_company,
"Do_Not_Contact__c" AS do_not_contact,
"ft_utm_channel__c" AS channel_first_touch,
"lt_utm_channel__c" AS channel_last_touch,
"lt_utm_medium__c" AS medium_last_touch,
"lt_utm_source__c" AS source_last_touch,
"lt_utm_campaign__c" AS campaign_last_touch,
"Form_Consent_Opt_In__c" AS form_consent_opt_in,
"Channel_Lead_Creation__c" AS channel_lead_creation,
"Channel_Lead_Creation__c" AS lead_channel_forecast,
"Medium_Lead_Creation__c" AS medium_lead_creation,
"Source_Lead_Creation__c" AS source_lead_creation,
"Campaign_Lead_Creation__c" AS campaign_lead_creation,
"Legitimate_Basis__c" AS legitimate_basis,
"Hand_Raiser__c" AS is_hand_raiser,
"ft_subchannel__c" AS subchannel_first_touch,
"lt_subchannel__c" AS subchannel_last_touch,
"lc_subchannel__c" AS subchannel_lead_creation,
"ft_offerAsset_type__c" AS offer_asset_type_first_touch,
"ft_offerAsset_subtype__c" AS offer_asset_subtype_first_touch,
"ft_offerAsset_topic__c" AS offer_asset_topic_first_touch,
"ft_offerAsset_name__c" AS offer_asset_name_first_touch,
"lc_offerAsset_type__c" AS offer_asset_type_lead_creation,
"lc_offerAsset_subtype__c" AS offer_asset_subtype_lead_creation,
"lc_offerAsset_topic__c" AS offer_asset_topic_lead_creation,
"lc_offerAsset_name__c" AS offer_asset_name_lead_creation,
"lt_offerAsset_type__c" AS offer_asset_type_last_touch,
"lt_offerAsset_subtype__c" AS offer_asset_subtype_last_touch,
"lt_offerAsset_topic__c" AS offer_asset_topic_last_touch,
"lt_offerAsset_name__c" AS offer_asset_name_last_touch,
"de_Current_CRM__c" AS current_crm,
"de_Current_Marketing_Automation__c" AS current_ma,
"Engagement_Level__c" AS engagement_level,
"ft_utm_medium__c" AS medium_first_touch,
"ft_utm_source__c" AS source_first_touch,
"ft_utm_campaign__c" AS campaign_first_touch,
CASE WHEN "AnnualRevenue" <= 49999999 THEN 'SMB'
     WHEN "AnnualRevenue" > 49999999 AND "AnnualRevenue" <= 499999999 THEN 'Mid-Market'
     WHEN "AnnualRevenue" > 499999999 THEN 'Enterprise'
     END AS company_size_rev 
--"X9883_Lead_Score__c" AS lead_score,
--de_industry__c AS industry,
FROM base
WHERE base."OwnerId" != '00Ga0000003Nugr' -- AO-Fake Leads
)

SELECT 
*
FROM final