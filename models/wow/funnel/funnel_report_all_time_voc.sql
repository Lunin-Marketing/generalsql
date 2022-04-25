{{ config(materialized='table') }}

WITH base AS (

    SELECT DISTINCT
        opp_voc_source_xf.opportunity_id AS voc_id,
        acv,
        opp_voc_source_xf.negotiation_date AS sqo_date,
        account_global_region,
        opp_lead_source,
        company_size_rev
    FROM {{ref('opp_voc_source_xf')}}
    WHERE type = 'New Business'

)

SELECT *
FROM base