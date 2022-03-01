{{ config(materialized='table') }}

WITH last_12_weeks AS (

SELECT DISTINCT
    week 
FROM {{ref('date_base_xf')}}
WHERE day BETWEEN CURRENT_DATE-84 AND CURRENT_DATE-7

), final AS (
    
    SELECT
        last_12_weeks.week,
        COUNT(DISTINCT sql_id) AS sqls
    FROM {{ref('sql_source_xf')}}
    LEFT JOIN {{ref('date_base_xf')}} ON
    sql_source_xf.created_date=date_base_xf.day
    LEFT JOIN last_12_weeks ON 
    date_base_xf.week=last_12_weeks.week
    WHERE last_12_weeks.week IS NOT null
    AND created_date IS NOT null
    AND stage_name NOT IN ('Closed - Duplicate','Closed - Admin Removed')
    AND type = 'New Business'
    GROUP BY 1

)

SELECT
    week,
    SUM(sqls) AS sqls
FROM final
GROUP BY 1