{{ config(materialized='table') }}

WITH base AS (

    SELECT DISTINCT
        opp_demo_source_xf.opportunity_id AS demo_id,
        CONCAT('https://acton.my.salesforce.com/',opp_demo_source_xf.opportunity_id) AS demo_url,
        acv,
        opp_demo_source_xf.demo_date AS demo_date,
        company_size_rev,
        account_global_region,
        opp_lead_source,
        segment,
        industry,
        channel_bucket
    FROM {{ref('opp_demo_source_xf')}}
    WHERE type = 'New Business'

)

SELECT *
FROM base