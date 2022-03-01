{{ config(materialized='table') }}

WITH current_week AS (

    SELECT
        week
    FROM {{ref('date_base_xf')}}
    WHERE day=CURRENT_DATE-7

), base AS (

    SELECT DISTINCT
        opp_demo_source_ss_xf.opportunity_id AS demo_id,
        acv,
        opp_demo_source_ss_xf.discovery_date AS sqo_date,
        country
    FROM {{ref('opp_demo_source_ss_xf')}}
    LEFT JOIN {{ref('date_base_xf')}} ON
    opp_demo_source_ss_xf.discovery_date=date_base_xf.day
    LEFT JOIN current_week ON 
    date_base_xf.week=current_week.week
    WHERE current_week.week IS NOT null
    AND type = 'New Business'

)

SELECT *
FROM base