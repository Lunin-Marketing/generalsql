

WITH base AS (
    SELECT DISTINCT
        person_source_xf.person_id,
        channel_lead_creation,
        medium_lead_creation,
        source_lead_creation--,
        -- channel_bucket,
        -- task_subject,
        -- task_type,
        -- task_created_date
    FROM "acton"."dbt_actonmarketing"."person_source_xf"
    -- LEFT JOIN "acton"."dbt_actonmarketing"."task_source_xf" ON
    -- person_source_xf.person_id=task_source_xf.person_id
    WHERE marketing_created_date >= '2022-02-01'
    AND channel_bucket = 'Other'
    -- AND channel_lead_creation IS null
    -- AND medium_lead_creation IS null
    -- AND source_lead_creation IS null
    -- AND offer_asset_name_lead_creation IS null
    -- AND offer_asset_topic_lead_creation IS null
    

)

SELECT 
    COUNT(DISTINCT person_id) AS leads,
    channel_lead_creation,
    medium_lead_creation,
    source_lead_creation
FROM base
GROUP BY 2,3,4