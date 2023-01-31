{{ config(materialized='table') }}

SELECT
    person_id,
    channel_lead_creation,
    medium_lead_creation,
    source_lead_creation
FROM person_source_xf
WHERE channel_bucket = 'Other'
AND marketing_created_date >= '2023-01-01'