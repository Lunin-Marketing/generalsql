{{ config(materialized='table') }}

WITH base AS (

    SELECT DISTINCT
        opp_closing_source_xf.opportunity_id AS closing_id,
        acv,
        opp_closing_source_xf.discovery_date AS closing_date,
        comapny_size_rev,
        account_global_region,
        opp_lead_source
    FROM {{ref('opp_closing_source_xf')}}
    AND type = 'New Business'

)

SELECT *
FROM base