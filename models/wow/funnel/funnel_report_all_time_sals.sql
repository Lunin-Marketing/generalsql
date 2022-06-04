{{ config(materialized='table') }}

WITH base AS (

    SELECT DISTINCT
        sal_source_xf.person_id AS sal_id,
        CONCAT('https://acton.my.salesforce.com/',sal_source_xf.person_id) AS sal_url,
        sal_source_xf.working_date AS sal_date,
        global_region,
        company_size_rev,
        lead_source,
        segment,
        industry,
        channel_bucket
    FROM {{ref('sal_source_xf')}}

)

SELECT *
FROM base