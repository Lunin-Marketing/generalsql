{{ config(materialized='table') }}

WITH base AS (
SELECT *
FROM "acton".salesforce."opportunity"

), final AS (
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
DATE_TRUNC('day',created_date)::Date AS created_date,
DATE_TRUNC('day',last_modified_date)::Date AS last_modified_date,
system_modstamp AS systemmodstamp,
contact_id,
contract_id,
crm_c AS opp_crm,
renewal_type_c AS renewal_type,
renewal_acv_value_c AS renewal_acv_value,
channel_lead_creation_c AS opp_channel_lead_creation,
medium_lead_creation_c AS opp_medium_lead_creation,
DATE_TRUNC('day',discovery_date_c)::Date AS discovery_date,
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
DATE_TRUNC('day',sql_date_c)::Date AS sql_date,
DATE_TRUNC('day',discovery_call_scheduled_date_time_c)::Date AS discovery_call_scheduled_datetime,
DATE_TRUNC('day',discovery_call_completed_date_time_c)::Date AS discovery_call_completed_datetime,
ao_account_id_c AS ao_account_id,
lead_id_converted_from_c AS lead_id_converted_from,
age_in_days AS age,
opportunity_type_detail_c AS opp_type_details,
DATE_TRUNC('day',close_date)::Date AS close_date,
source_lead_creation_c AS opp_source_lead_creation,
oc_utm_campaign_c AS opp_campaign_opportunity_creation,
forecast_category,
ft_utm_campaign_c AS opp_campaign_first_touch,  
acv_deal_size_override_c AS acv_deal_size_override,
lead_grade_at_conversion_c AS lead_grade_at_conversion,
renewal_stage_c AS renewal_stage,
created_by_id,
quota_credit_renewal_c AS quota_credit_renewal,
sbqq_renewed_contact_c AS renewed_contract_id,
quota_credit_c AS quota_credit,
sbqq_primary_quote_c AS primary_quote_id,
quota_credit_new_business_c AS quota_credit_new_business,
quota_credit_one_time_c AS quota_credit_one_time,

-- "Age__c" AS age,
-- "Renewal_ACV__c" AS renewal_acv,
-- "ACV__c" AS acv, --not present? 
-- "ACV_Deal_Size_USD__c" AS acv_deal_size_usd, --not present? 
CASE WHEN "ACV_Deal_Size_USD__c" <= '9999' THEN '< 10K'
     WHEN "ACV_Deal_Size_USD__c" > '9999' AND "ACV_Deal_Size_USD__c" <= '14999' THEN '10-15K'
     WHEN "ACV_Deal_Size_USD__c" > '14999' AND "ACV_Deal_Size_USD__c" <= '19999' THEN '15-20K'
     WHEN "ACV_Deal_Size_USD__c" > '19999' AND "ACV_Deal_Size_USD__c" <= '24999' THEN '20-25K'
     WHEN "ACV_Deal_Size_USD__c" > '24999' AND "ACV_Deal_Size_USD__c" <= '29999' THEN '25-30K'
     ELSE '30K+'
     END AS deal_size_range,
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
    END AS channel_bucket
FROM base
)

SELECT 
*
FROM final