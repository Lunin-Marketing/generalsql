{{ config(materialized='table') }}

WITH current_week AS (

    SELECT
        week
    FROM {{ref('date_base_xf')}}
    WHERE day=CURRENT_DATE-7

), base AS (

    SELECT DISTINCT
        sql_id,
        sql_source_xf.created_date AS sql_date,
        country,
        account_global_region
    FROM {{ref('sql_source_xf')}}
    LEFT JOIN {{ref('date_base_xf')}} ON
    sql_source_xf.created_date::Date=date_base_xf.day
    LEFT JOIN current_week ON 
    date_base_xf.week=current_week.week
    WHERE current_week.week IS NOT null
    AND type = 'New Business'

)

SELECT *
FROM base