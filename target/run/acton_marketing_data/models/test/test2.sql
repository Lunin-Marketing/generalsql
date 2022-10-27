

  create  table "acton"."dbt_actonmarketing"."test2__dbt_tmp"
  as (
    

WITH base AS (
    SELECT DISTINCT
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
    FROM "acton"."dbt_actonmarketing"."lead_source_xf"

)

SELECT 
    channel_lead_creation,
    medium_lead_creation,
    source_lead_creation
FROM base
WHERE channel_bucket = 'Other'
  );