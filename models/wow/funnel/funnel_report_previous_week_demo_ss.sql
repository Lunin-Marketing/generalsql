{{ config(materialized='table') }}

WITH previous_week AS (

    SELECT
        week 
    FROM {{ref('date_base_xf')}}
    WHERE day=CURRENT_DATE-14

), base AS (

    SELECT DISTINCT
        opp_demo_source_ss_xf.opportunity_id AS demo_id,
        acv,
        opp_demo_source_ss_xf.demo_date AS demo_date,
        country,
        account_global_region
    FROM {{ref('opp_demo_source_ss_xf')}}
    LEFT JOIN {{ref('date_base_xf')}} ON
    opp_demo_source_ss_xf.demo_date=date_base_xf.day
    LEFT JOIN previous_week ON 
    date_base_xf.week=previous_week.week
    WHERE previous_week.week IS NOT null
    AND type = 'New Business'

)

SELECT *
FROM base