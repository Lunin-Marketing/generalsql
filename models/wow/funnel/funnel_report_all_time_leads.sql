{{ config(materialized='table') }}

WITH base AS (

    SELECT DISTINCT
        person_source_xf.person_id AS lead_id,
        CONCAT('https://acton.my.salesforce.com/',person_source_xf.person_id) AS lead_url,
        person_source_xf.marketing_created_date AS created_date,
        global_region,
        company_size_rev,
        lead_source,
        segment,
        industry,
        channel_bucket
    FROM {{ref('person_source_xf')}}
    WHERE marketing_created_date IS NOT null

)

SELECT *
FROM base