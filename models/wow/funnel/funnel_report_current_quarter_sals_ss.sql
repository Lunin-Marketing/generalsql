{{ config(materialized='table') }}

WITH current_quarter AS (

    SELECT
        fy,
        quarter 
    FROM {{ref('date_base_xf')}}
    WHERE day=CURRENT_DATE

), base AS (

    SELECT DISTINCT
        sal_source_ss_xf.lead_id AS sal_id,
        sal_source_ss_xf.working_date AS sal_date,
        week,
        global_region
    FROM {{ref('sal_source_ss_xf')}}
    LEFT JOIN {{ref('date_base_xf')}} ON
    sal_source_ss_xf.working_date=date_base_xf.day
    LEFT JOIN current_quarter ON 
    date_base_xf.quarter=current_quarter.quarter
    WHERE current_quarter.quarter IS NOT null

)

SELECT *
FROM base