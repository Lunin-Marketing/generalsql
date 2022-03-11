{{ config(materialized='table') }}

WITH previous_week AS (

    SELECT
        week 
    FROM {{ref('date_base_xf')}}
    WHERE day=CURRENT_DATE-14

), base AS (

    SELECT DISTINCT
        person_source_xf.person_id AS lead_id,
        person_source_xf.marketing_created_date AS created_date,
        country,
        global_region
    FROM {{ref('person_source_xf')}}
    LEFT JOIN {{ref('date_base_xf')}} ON
    person_source_xf.marketing_created_date=date_base_xf.day
    LEFT JOIN previous_week ON 
    date_base_xf.week=previous_week.week
    WHERE previous_week.week IS NOT null

)

SELECT *
FROM base