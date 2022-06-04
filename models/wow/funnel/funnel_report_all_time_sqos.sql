{{ config(materialized='table') }}

WITH base AS (

    SELECT DISTINCT
        sqo_source_xf.opportunity_id AS sqo_id,
        CONCAT('https://acton.my.salesforce.com/',sqo_source_xf.opportunity_id) AS sqo_url,
        sqo_source_xf.discovery_date AS sqo_date,
        acv,
        account_global_region,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        company_size_rev
    FROM {{ref('sqo_source_xf')}}
    WHERE type = 'New Business'

)

SELECT *
FROM base