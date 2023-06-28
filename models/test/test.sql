{{ config(materialized='table') }}

SELECT DISTINCT
    channel_lead_creation,
    medium_lead_creation,
    source_lead_creation,
    channel_bucket,
    channel_bucket_details    
FROM {{ref('person_source_xf')}}