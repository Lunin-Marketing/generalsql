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
        sal_source_xf.person_id AS sal_id,
        sal_source_xf.working_date AS working_date,
        rolling_3mo.week,
        global_region
    FROM {{ref('sal_source_xf')}}
    LEFT JOIN {{ref('date_base_xf')}} ON
    sal_source_xf.working_date=date_base_xf.day
    LEFT JOIN rolling_3mo ON 
    date_base_xf.week=rolling_3mo.week
    WHERE rolling_3mo.week IS NOT null

)

SELECT *
FROM base