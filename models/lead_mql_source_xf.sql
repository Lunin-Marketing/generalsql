{{ config(materialized='table') }}

select
lead_id,
email,
lead_source,
is_converted,
hand_raiser__c AS is_hand_raiser,
mql_created_date__c AS mql_created_date,
mql_most_recent_date__c AS mql_most_recent_date,
lead_status AS lead_status
from "defaultdb".public.lead_source_20210517
