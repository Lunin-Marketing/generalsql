

WITH base AS (

    SELECT DISTINCT
        person_source_xf.person_id AS lead_id,
        CONCAT('https://acton.my.salesforce.com/',person_source_xf.person_id) AS lead_url,
        person_source_xf.marketing_created_date AS created_date,
    CASE
        WHEN global_region IS null THEN 'blank'
        ELSE global_region
    END AS global_region,
    CASE
        WHEN company_size_rev IS null THEN 'blank'
        ELSE company_size_rev
    END AS company_size_rev,
    CASE
        WHEN lead_source IS null THEN 'blank'
        ELSE lead_source
    END AS lead_source,
    CASE
        WHEN segment IS null THEN 'blank'
        ELSE segment
    END AS segment,
    CASE
        WHEN industry IS null THEN 'blank'
        ELSE industry
    END AS industry,
    CASE
        WHEN industry_bucket IS null THEN 'blank'
        ELSE industry_bucket
    END AS industry_bucket,
    CASE
        WHEN channel_bucket IS null THEN 'blank'
        ELSE channel_bucket
    END AS channel_bucket
    FROM "acton"."dbt_actonmarketing"."person_source_xf"
    WHERE marketing_created_date IS NOT null

)

SELECT *
FROM base