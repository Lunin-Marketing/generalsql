{{ config(materialized='table') }}

SELECT
    person_id,
    lead_source,
    channel_lead_creation,
    medium_lead_creation,
    source_lead_creation
FROM person_source_xf
WHERE lead_source NOT IN ('Marketing','Sales','SDR','CSM','Channel')

-- SELECT
--     person_id,
--     lead_source,
--     channel_lead_creation,
--     medium_lead_creation
-- FROM person_source_xf
-- WHERE lead_source = 'Marketing'
-- AND LOWER(channel_lead_creation) = 'prospecting'