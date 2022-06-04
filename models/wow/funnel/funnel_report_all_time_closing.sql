{{ config(materialized='table') }}

WITH base AS (

    SELECT DISTINCT
        opp_closing_source_xf.opportunity_id AS closing_id,
        CONCAT('https://acton.my.salesforce.com/',opp_closing_source_xf.opportunity_id) AS closing_url,
        acv,
        opp_closing_source_xf.closing_date AS closing_date,
        company_size_rev,
        account_global_region,
        opp_lead_source,
        segment,
        industry,
        channel_bucket
    FROM {{ref('opp_closing_source_xf')}}
    WHERE type = 'New Business'

)

SELECT *
FROM base