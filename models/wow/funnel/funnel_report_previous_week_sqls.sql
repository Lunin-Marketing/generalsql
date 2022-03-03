{{ config(materialized='table') }}

WITH previous_week AS (

    SELECT
        week
    FROM {{ref('date_base_xf')}}
    WHERE day=CURRENT_DATE-14

), base AS (

    SELECT DISTINCT
        sql_source_xf.sql_id,
        sql_source_xf.created_date AS sql_date,
        country,
        account_global_region
    FROM {{ref('sql_source_xf')}}
    LEFT JOIN {{ref('date_base_xf')}} ON
    sql_source_xf.created_date=date_base_xf.day
    LEFT JOIN previous_week ON 
    date_base_xf.week=previous_week.week
    WHERE previous_week.week IS NOT null
    AND type = 'New Business'

)

SELECT *
FROM base