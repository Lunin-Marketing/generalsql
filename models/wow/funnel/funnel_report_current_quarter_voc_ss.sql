{{ config(materialized='table') }}

WITH current_quarter AS (

    SELECT
        fy,
        quarter 
    FROM {{ref('date_base_xf')}}
    WHERE day=CURRENT_DATE

), base AS (

    SELECT DISTINCT
        opp_voc_source_ss_xf.opportunity_id AS voc_id,
        acv,
        opp_voc_source_ss_xf.negotiation_date AS sqo_date,
        country
    FROM {{ref('opp_voc_source_ss_xf')}}
    LEFT JOIN {{ref('date_base_xf')}} ON
    opp_voc_source_ss_xf.negotiation_date=date_base_xf.day
    LEFT JOIN current_quarter ON 
    date_base_xf.quarter=current_quarter.quarter
    WHERE current_quarter.quarter IS NOT null
    AND type = 'New Business'

)

SELECT *
FROM base