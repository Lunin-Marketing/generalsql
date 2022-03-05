{{ config(materialized='table') }}

WITH current_quarter AS (

    SELECT
        fy,
        quarter 
    FROM {{ref('date_base_xf')}}
    WHERE day=CURRENT_DATE

), base AS (

    SELECT DISTINCT
        opp_demo_source_ss_xf.opportunity_id AS demo_id,
        acv,
        opp_demo_source_ss_xf.discovery_date AS sqo_date,
        country,
        account_global_region
    FROM {{ref('opp_demo_source_ss_xf')}}
    LEFT JOIN {{ref('date_base_xf')}} ON
    opp_demo_source_ss_xf.discovery_date=date_base_xf.day
    LEFT JOIN current_quarter ON 
    date_base_xf.quarter=current_quarter.quarter
    WHERE current_quarter.quarter IS NOT null
    AND type = 'New Business'

)

SELECT *
FROM base