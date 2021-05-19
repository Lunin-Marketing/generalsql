{{ config(materialized='table') }}

SELECT
lead_id,
email,
lead_source,
is_converted,
hand_raiser__c AS is_hand_raiser,
mql_created_date__c AS mql_created_date,
mql_most_recent_date__c AS mql_most_recent_date,
lead_status
FROM "defaultdb".public.lead_source_20210517
WHERE mql_most_recent_date__c IS NOT null
