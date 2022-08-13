{{ config(materialized='table') }}

WITH sqopipe_target AS (

    SELECT 
        kpi_month,
        kpi_region,
        kpi_target AS sqopipe_target
    FROM {{ref('kpi_targets')}}
    WHERE kpi = 'target_new_cw_pipe'
    --WHERE kpi = 'target_sqo_pipe'
    

), actual_sqopipe AS (

    SELECT
        DATE_TRUNC('month',sqo_date)::Date AS sqopipe_month,
        CASE 
            WHEN account_global_region LIKE 'NA-%' THEN 'NA'
            WHEN account_global_region IN ('EUROPE') THEN 'EMEA'
            ELSE null
        END AS region,
        SUM(acv) AS sqopipe
    FROM {{ref('funnel_report_all_time_sqos')}}
    WHERE sqo_date >= '2022-04-01'
    GROUP BY 1,2
)

SELECT
    sqopipe_month,
    actual_sqopipe.region,
    sqopipe,
    sqopipe_target,
    sqopipe/sqopipe_target AS actual_vs_target
FROM actual_sqopipe
LEFT JOIN sqopipe_target ON
actual_sqopipe.sqopipe_month=sqopipe_target.kpi_month
AND actual_sqopipe.region=sqopipe_target.kpi_region
WHERE region IS NOT null