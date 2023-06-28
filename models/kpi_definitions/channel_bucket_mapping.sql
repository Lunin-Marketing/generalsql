{{ config(materialized='table') }}

SELECT DISTINCT
    channel_bucket,
    channel_lead_creation,
    medium_lead_creation,
    source_lead_creation
FROM {{ref('person_source_xf')}}
WHERE channel_bucket IS NOT NULL
ORDER BY 1