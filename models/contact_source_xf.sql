{{ config(materialized='table') }}
WITH base AS (
SELECT *
FROM "acton".public."Contact"

), final AS (
SELECT 
"Id" AS contact_id,
"IsDeleted" AS is_deleted,
"AccountId" AS account_id,
"FirstName" AS first_name,
"LastName" AS last_name, 
"OwnerId" AS owner_id,
"Email" AS email,
"X9883_Lead_Score__c" AS lead_score,
"lc_offerAsset_name__c" AS offer_asset_name_lead_creation,
"ft_offerAsset_name__c" AS offer_asset_name_first_touch,
"lt_offerAsset_name__c" AS offer_asset_name_last_touch,
"ft_offerAsset_subtype__c" AS offer_asset_subtype_first_touch,
"lt_offerAsset_subtype__c" AS offer_asset_subtype_last_touch,
"lc_offerAsset_subtype__c" AS offer_asset_subtype_lead_creation,
"ft_offerAsset_topic__c" AS offer_asset_topic_first_touch,
"lt_offerAsset_topic__c" AS offer_asset_topic_last_touch,
"lc_offerAsset_topic__c" AS offer_asset_topic_lead_creation,
"ft_offerAsset_type__c" AS offer_asset_type_first_touch,
"lt_offerAsset_type__c" AS offer_asset_type_last_touch,
"lc_offerAsset_type__c" AS offer_asset_type_lead_creation,
"ft_utm_campaign__c" AS campaign_first_touch,
"lt_utm_campaign__c" AS campaign_last_touch,
"Campaign_Lead_Creation__c" AS campaign_lead_creation,
"ft_utm_channel__c" AS channel_first_touch,
"lt_utm_channel__c" AS channel_last_touch,
"Channel_Lead_Creation__c" AS channel_lead_creation,
"ft_subchannel__c" AS subchannel_first_touch,
"lt_subchannel__c" AS subchannel_last_touch,
"lc_subchannel__c" AS subchannel_lead_creation,
"ft_utm_medium__c" AS medium_first_touch,
"lt_utm_medium__c" AS medium_last_touch,
"Medium_Lead_Creation__c" AS medium_lead_creation,
"ft_utm_source__c" AS source_first_touch,
"lt_utm_source__c" AS source_last_touch,
"Source_Lead_Creation__c" AS source_lead_creation,
"LeadSource" AS lead_source,
"MailingPostalCode" AS mailing_postal_code,
"MailingCountry" AS mailing_country,
"Account_Owner_ID__c" AS account_owner_id,
"Account_Owner__c" AS account_owner,
"No_Longer_with_Company__c" AS is_no_longer_with_company,
"Current_Customer__c" AS is_current_customer,
"Title" AS title,
"Engagement_Level__c" AS engagement_level,
"Firmographic_Demographic_Lead_Grade__c" AS firmographic_demographic_lead_grade,
"Firmographic_Demographic_Lead_Score__c" AS firmographic_demographic_lead_score,
"Hand_Raiser__c" AS is_hand_raiser,
"Was_a_Handraiser_Lead__c" AS was_a_handraiser_lead,
"MQL_Created_Date__c" AS mql_created_date,
"MQL_Most_Recent_Date__c" AS mql_most_recent_date,
"CreatedDate" AS created_date,
"Marketing_Lead_Creation_Date__c" AS marketing_created_date,
"Contact_Status__c" AS contact_status,
"LeadID_Converted_From__c" AS lead_id_converted_from,
"Is_Renewal_Contact__c" AS is_renewal_contact,
--"Renewal_Contact__c" AS 
"Owner_ID__c" AS owner_id2,
"Account_CSM__c" AS account_csm,
"Contact_Role__c" AS contact_role,
"Account_CSM_ID__c" AS account_csm_id,
"Primary_Contact__c" AS is_primary_contact,
"Account_SDR_Photo__c" AS account_sdr_photo,
"Account_SDR_Phone__c" AS account_sdr_phone,
"Account_SDR_Calendly__c" AS account_sdr_calendly,
"Account_SDR_Title__c" AS account_sdr_title,
"Account_Owner_Email__c" AS account_owner_email,
"Account_CSM_Email__c" AS account_csm_email,
"Account_Deliverability_Consultant_Email__c" AS account_deliverability_consultant_email,
"Account_Deliverability_Consultant__c" AS account_deliverability_consultant,
"Account_SDR_Full_Name__c" AS account_sdr_full_name,
"Account_SDR_Email__c" AS account_sdr_email,
"SF_Id_18_Char__c" AS contact_id_18,
"Owner_Email__c" AS owner_email,
annual_revenue,
CASE WHEN annual_revenue <= 49999999 THEN 'SMB'
     WHEN annual_revenue > 49999999 AND annual_revenue <= 499999999 THEN 'Mid-Market'
     WHEN annual_revenue > 499999999 THEN 'Enterprise'
     END AS company_size_rev,
de_current_crm,
de_current_ma,
sdr
FROM base
LEFT JOIN "acton".dbt_actonmarketing.account_source_xf ON
base."AccountId"=account_source_xf.account_id
)

SELECT
*
FROM final