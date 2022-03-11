{{ config(materialized='table') }}

WITH rolling_3mo AS (

    SELECT DISTINCT
        week
    FROM {{ref('date_base_xf')}}
    WHERE day >= CURRENT_DATE-90
    AND day <= CURRENT_DATE
    ORDER BY 1

), base AS (

    SELECT DISTINCT
        opp_voc_source_xf.opportunity_id AS voc_id,
        opp_voc_source_xf.negotiation_date AS voc_date,
        rolling_3mo.week,
        account_global_region
    FROM {{ref('opp_voc_source_xf')}}
    LEFT JOIN {{ref('date_base_xf')}} ON
    opp_voc_source_xf.negotiation_date=date_base_xf.day
    LEFT JOIN rolling_3mo ON 
    date_base_xf.week=rolling_3mo.week
    WHERE rolling_3mo.week IS NOT null

)

SELECT *
FROM base