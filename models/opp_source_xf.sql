{{ config(materialized='table') }}
WITH base AS (
SELECT *
FROM "acton".public."Opportunity"

), final AS (
SELECT 
"Id" AS opportunity_id,
"IsDeleted" AS is_deleted,
"AccountId" AS account_id,
"Name" AS opportunity_name,
"StageName" AS stage_name,
"Amount" AS amount,
DATE_TRUNC('day',"CloseDate")::Date AS close_date,
"Type" AS type,
"LeadSource" AS opp_lead_source,
"IsClosed" AS is_closed,
"IsWon" AS is_won,
"ForecastCategory" AS forecast_category,
"OwnerId" AS owner_id, 
"CreatedById" AS created_by_id,
DATE_TRUNC('day',"CreatedDate")::Date AS created_date,
DATE_TRUNC('day',"LastModifiedDate")::Date AS last_modified_date,
"ContactId" AS contact_id,
"Renewal_Type__c" AS renewal_type,
"Renewal_ACV_Value__c" AS renewal_acv_value,
"Channel_Lead_Creation__c" AS opp_channel_lead_creation,
"Medium_Lead_Creation__c" AS opp_medium_lead_creation,
"Source_Lead_Creation__c" AS opp_source_lead_creation,
"oc_utm_channel__c" AS opp_channel_opportunity_creation, 
"oc_utm_medium__c" AS opp_medium_opportunity_creation,
"Lead_Grade_at_Conversion__c" AS lead_grade_at_conversion,
"oc_utm_source__c" AS opp_source_opportunity_creation, 
"oc_utm_campaign__c" AS opp_campaign_opportunity_creation,
"ft_utm_campaign__c" AS opp_campaign_first_touch, 
"ft_utm_channel__c" AS opp_channel_first_touch,
"ft_utm_medium__c" AS opp_medium_first_touch,
"ft_utm_source__c" AS opp_source_first_touch, 
"Opportunity_Type_Detail__c" AS opp_type_details,
"oc_offerAsset_type__c" AS opp_offer_asset_type_opportunity_creation,
"oc_offerAsset_subtype__c" AS opp_offer_asset_subtype_opportunity_creation,
"oc_offerAsset_topic__c" AS opp_offer_asset_topic_opportunity_creation,
"oc_offerAsset_name__c" AS opp_offer_asset_name_opportunity_creation,
"LeadID_Converted_From__c" AS lead_id_converted_from,
"ft_offerAsset_name__c" AS opp_offer_asset_name_first_touch, 
"ft_offerAsset_subtype__c" AS opp_offer_asset_subtype_first_touch, 
"ft_offerAsset_topic__c" AS opp_offer_asset_topic_first_touch, 
"ft_offerAsset_type__c" AS opp_offer_asset_type_first_touch, 
"lc_offerAsset_name__c" AS opp_offer_asset_name_lead_creation, 
"lc_offerAsset_subtype__c" AS opp_offer_asset_subtype_lead_creation, 
"lc_offerAsset_topic__c" AS opp_offer_asset_topic_lead_creation, 
"lc_offerAsset_type__c" AS opp_offer_asset_type_lead_creation,
"Age__c" AS age,
"ft_subchannel__c" AS opp_subchannel_first_touch,
"oc_subchannel__c" AS opp_subchannel_opportunity_creation,
"lc_subchannel__c" AS opp_subchannel_lead_creation, 
DATE_TRUNC('day',"Discovery_Call_Scheduled_Date__c")::Date AS discovery_call_date,
DATE_TRUNC('day',"Discovery_Call_Scheduled_Date_Time__c")::Date AS discovery_call_scheduled_datetime,
DATE_TRUNC('day',"Discovery_Call_Completed_Date_Time__c")::Date AS discovery_call_completed_datetime,
"SQL_Status_Reason__c" AS sql_status_reason,
"Opportunity_Status__c" AS opportunity_status,
"Renewal_Stage__c" AS renewal_stage,
"Renewal_ACV__c" AS renewal_acv,
"ACV__c" AS acv,
"ACV_Deal_Size_USD__c" AS acv_deal_size_usd,
DATE_TRUNC('day',"Discovery_Date__c")::Date AS discovery_date,
CASE WHEN "ACV_Deal_Size_USD__c" <= '9999' THEN '< 10K'
     WHEN "ACV_Deal_Size_USD__c" > '9999' AND "ACV_Deal_Size_USD__c" <= '14999' THEN '10-15K'
     WHEN "ACV_Deal_Size_USD__c" > '14999' AND "ACV_Deal_Size_USD__c" <= '19999' THEN '15-20K'
     WHEN "ACV_Deal_Size_USD__c" > '19999' AND "ACV_Deal_Size_USD__c" <= '24999' THEN '20-25K'
     WHEN "ACV_Deal_Size_USD__c" > '24999' AND "ACV_Deal_Size_USD__c" <= '29999' THEN '25-30K'
     ELSE '30K+'
     END AS deal_size_range
FROM base
)

SELECT 
*
FROM final