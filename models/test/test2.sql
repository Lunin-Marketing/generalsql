{{ config(materialized='table') }}

WITH base AS (
    SELECT DISTINCT
        lead_id,
        channel_lead_creation,
        medium_lead_creation,
        source_lead_creation,
        CASE 
            WHEN LOWER(channel_lead_creation) = 'organic' THEN 'Organic'
            WHEN LOWER(channel_lead_creation) IS null THEN 'Unknown'
            WHEN LOWER(channel_lead_creation) = 'social' AND LOWER(medium_lead_creation) = 'social-organic' THEN 'Social - Organic'
            WHEN LOWER(channel_lead_creation) = 'social' AND LOWER(medium_lead_creation) = 'social-paid' THEN 'Paid Social'
            WHEN LOWER(channel_lead_creation) = 'ppc' THEN 'PPC/Paid Search'
            WHEN LOWER(channel_lead_creation) = 'email' AND LOWER(source_lead_creation) like '%act-on%' THEN 'Paid Email' 
            WHEN LOWER(channel_lead_creation) = 'ppl' AND LOWER(medium_lead_creation) = 'syndication partner' THEN 'PPL'
            WHEN LOWER(channel_lead_creation) IN ('prospecting','ppl') AND LOWER(medium_lead_creation) = 'intent partner' THEN 'Intent Partners'
            WHEN LOWER(channel_lead_creation) = 'event' THEN 'Events and Trade Shows'
            WHEN LOWER(channel_lead_creation) = 'partner' THEN 'Partners'
            ELSE 'Other'
        END AS channel_bucket
    FROM {{ref('lead_source_xf')}}
    WHERE marketing_created_date >= '2022-01-01'

)

SELECT 
    COUNT(DISTINCT lead_id) AS leads,
    channel_lead_creation,
    medium_lead_creation,
    source_lead_creation
FROM base
WHERE channel_bucket = 'Other'
GROUP BY 2,3,4