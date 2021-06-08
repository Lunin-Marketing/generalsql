{{ config(materialized='table') }}
WITH base AS (
--SELECT *
--FROM "defaultdb".public.contact_source_2020
--UNION ALL
SELECT *
FROM "acton".public.contact_source_20210524
)

SELECT 
account_id,
parent_account_id,
contact_id,
owner_id__c AS owner_id,
email,
x9883_lead_Score__c AS lead_score,
ft_offerasset_name__c AS offer_asset_name_first_touch,
lt_offerasset_name__c AS offer_asset_name_last_touch,
lc_offerasset_name__c AS offer_asset_name_lead_creation,
ft_offerasset_subtype__c AS offer_asset_subtype_first_touch,
lt_offerasset_subtype__c AS offer_asset_subtype_last_touch,
lc_offerasset_subtype__c AS offer_asset_subtype_lead_creation,
ft_offerasset_topic__c AS offer_asset_topic_first_touch,
lt_offerasset_topic__c AS offer_asset_topic_last_touch,
lc_offerasset_topic__c AS offer_asset_topic_lead_creation,
ft_offerasset_type__c AS offer_asset_type_first_touch,
lt_offerasset_type__c AS offer_asset_type_last_touch,
lc_offerasset_type__c AS offer_asset_type_lead_creation,
ft_utm_campaign__c AS campaign_first_touch,
lt_utm_campaign__c AS campaign_last_touch,
ft_utm_channel__c AS channel_first_touch,
lt_utm_channel__c AS channel_last_touch,
channel_lead_creation__c AS channel_lead_creation,
ft_utm_medium__c AS medium_first_touch,
lt_utm_medium__c AS medium_last_touch,
medium_lead_creation__c AS medium_lead_creation,
ft_utm_source__c AS source_first_touch,
lt_utm_source__c AS source_last_touch,
source_lead_creation__c AS source_lead_creation,
ft_subchannel__c AS subchannel_first_touch,
lt_subchannel__c AS subchannel_last_touch,
lc_subchannel__c AS subchannel_lead_creation,
lead_source,
mailing_postal_code,
mailing_country,
account_owner__c AS account_owner,
account_name,
annual_revenue,
current_customer_checkbox__c AS is_current_customer,
no_longer_with_company__c AS is_no_longer_with_company,
customer_since__c AS customer_since,
de_current_crm__c AS current_crm,
de_current_marketing_automation__c AS current_ma,
de_industry__c AS industry,
do_not_market__c AS is_do_not_market,
number_of_open_opps__c AS number_of_open_opps,
title,
engagement_level__c AS engagement_level,
firmographic_demographic_lead_grade__c AS firmographic_demographic_lead_grade,
firmographic_demographic_lead_score__c AS firmographic_demographic_lead_score,
hand_raiser__c AS is_hand_raiser,
mql_created_date__c AS mql_created_date,
mql_most_recent_date__c AS mql_most_recent_date

FROM base
