{{ config(materialized='table') }}

WITH current_quarter AS (

    SELECT
        fy,
        quarter 
    FROM {{ref('date_base_xf')}}
    WHERE day=CURRENT_DATE

), base AS (

    SELECT DISTINCT
        opp_demo_source_xf.opportunity_id AS demo_id,
        acv,
        opp_demo_source_xf.demo_date AS sqo_date,
        company_size_rev,
        account_global_region,
        opp_lead_source
    FROM {{ref('opp_demo_source_xf')}}
    AND type = 'New Business'

)

SELECT *
FROM base