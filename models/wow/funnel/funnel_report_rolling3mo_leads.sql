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
        person_source_xf.person_id AS lead_id,
        person_source_xf.marketing_created_date AS created_date,
        rolling_3mo.week,
        global_region
    FROM {{ref('person_source_xf')}}
    LEFT JOIN {{ref('date_base_xf')}} ON
    person_source_xf.marketing_created_date=date_base_xf.day
    LEFT JOIN rolling_3mo ON 
    date_base_xf.week=rolling_3mo.week
    WHERE rolling_3mo.week IS NOT null

)

SELECT *
FROM base