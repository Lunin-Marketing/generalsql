{{ config(materialized='table') }}

WITH won_pipe_target AS (

    SELECT 
        kpi_month,
        kpi_region,
        kpi_target AS won_pipe_target
    FROM {{ref('kpi_targets')}}
    WHERE kpi = 'target_cw_pipe'

), actual_won_pipe AS (

    SELECT
        DATE_TRUNC('month',won_date)::Date AS won_month,
        CASE 
            WHEN account_global_region LIKE 'NA-%' THEN 'NA'
            WHEN account_global_region IN ('EUROPE') THEN 'EMEA'
            ELSE null
        END AS region,
        SUM(acv) AS won_pipe
    FROM {{ref('funnel_report_all_time_won')}}
    WHERE won_date >= '2022-04-01'
    GROUP BY 1,2
)

SELECT
    won_month,
    actual_won_pipe.region,
    won_pipe,
    won_pipe_target,
    won_pipe/won_pipe_target AS actual_vs_target
FROM actual_won_pipe
LEFT JOIN won_pipe_target ON
actual_won_pipe.won_month=won_pipe_target.kpi_month
AND actual_won_pipe.region=won_pipe_target.kpi_region
WHERE region IS NOT null