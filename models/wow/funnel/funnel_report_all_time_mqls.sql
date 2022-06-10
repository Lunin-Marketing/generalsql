{{ config(materialized='table') }}

WITH base AS (

    SELECT DISTINCT
        lead_mql_source_xf.person_id AS mql_id,
        CONCAT('https://acton.my.salesforce.com/',lead_mql_source_xf.person_id) AS mql_url,
        lead_mql_source_xf.mql_created_date AS mql_date,
        global_region,
        company_size_rev,
        lead_source,
        segment,
        industry,
        channel_bucket
    FROM {{ref('lead_mql_source_xf')}}
    WHERE lead_mql_source_xf.mql_created_date IS NOT null

)

SELECT *
FROM base