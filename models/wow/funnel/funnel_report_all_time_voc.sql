{{ config(materialized='table') }}

WITH base AS (

    SELECT DISTINCT
        opp_voc_source_xf.opportunity_id AS voc_id,
        CONCAT('https://acton.my.salesforce.com/',opp_voc_source_xf.opportunity_id) AS voc_url,
        acv,
        opp_voc_source_xf.negotiation_date AS voc_date,
        account_global_region,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        company_size_rev
    FROM {{ref('opp_voc_source_xf')}}
    WHERE type = 'New Business'

)

SELECT *
FROM base