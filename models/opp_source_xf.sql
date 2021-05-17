{{ config(materialized='table') }}

select 
opportunity_id,
parent_account_id, 
account_id, 
owner_id, 
opportunity_name,
is_closed,
is_won, 
closedate AS close_date,
createddate AS created_date,
discovery_date__c AS discovery_date,
stagename AS stage_name, 
lead_source AS opp_lead_source,
acv_deal_size__c AS acv,
channel_lead_creation__c AS opp_channel_lead_creation,
medium_lead_creation__c AS opp_medium_lead_creation,
source_lead_creation__c AS opp_source_lead_creation,
lc_subchannel__c AS opp_subchannel_lead_creation, 
oc_utm_channel__c AS opp_channel_opportunity_creation, 
oc_utm_medium__c AS opp_medium_opportunity_creation,
oc_utm_source__c AS opp_source_opportunity_creation, 
oc_subchannel__c AS opp_subchannel_opportunity_creation,
channel_lead_creation__c AS opp_channel_forecast,
age,
annual_revenue AS opp_annual_revenue,
billing_country AS opp_billing_country, 
billing_postal_code AS opp_billing_postal_code,
primary_contact__c AS primary_contact, 
contact_email AS opp_contact_email,
ft_utm_channel__c AS opp_channel_first_touch, 
ft_utm_medium__c AS opp_medium_first_touch, 
ft_offerasset_name__c AS opp_offer_asset_name_first_touch, 
ft_offerasset_subtype__c AS opp_offer_asset_subtype_first_touch, 
ft_offerasset_topic__c AS opp_offer_asset_topic_first_touch, 
ft_offerasset_type__c AS opp_offer_asset_type_first_touch, 
ft_utm_source__c AS opp_source_first_touch, 
ft_subchannel__c AS opp_subchannel_first_touch, 
leandata__days_to_close__c AS days_to_close,
sbqq__renewal__c AS is_renewal,
renewal_acv,
segment,
type,
forecast_category__c AS forecast_category
from "defaultdb".public.opportunity_source_20210517