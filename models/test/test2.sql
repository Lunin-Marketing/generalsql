{{ config(materialized='table') }}

SELECT DISTINCT
person_id,
channel_lead_creation,
medium_lead_creation,
source_lead_creation,
channel_first_touch,
medium_first_touch,
source_first_touch
FROM person_source_xf
WHERE (channel_first_touch IS NOT null
    OR medium_first_touch IS NOT null
    OR source_first_touch IS NOT null)
AND (channel_lead_creation IS null
    OR medium_lead_creation IS null
    OR source_lead_creation IS null)
