{{ config(materialized='table') }}

WITH sqo_target AS (

    SELECT 
        kpi_month,
        kpi_region,
        kpi_target AS sqos_target
    FROM {{ref('kpi_targets')}}
    WHERE kpi = 'target_sqos'

), actual_sqos AS (

    SELECT
        DATE_TRUNC('month',sqo_date)::Date AS sqos_month,
        CASE 
            WHEN account_global_region LIKE 'NA-%' THEN 'NA'
            WHEN account_global_region IN ('EUROPE') THEN 'EMEA'
            ELSE null
        END AS region,
        COUNT(DISTINCT sqo_id) AS sqos
    FROM {{ref('funnel_report_all_time_sqos')}}
    WHERE sqo_date >= '2022-04-01'
    GROUP BY 1,2
)

SELECT
    sqos_month,
    actual_sqos.region,
    sqos,
    sqos_target,
    sqos/sqos_target AS actual_vs_target
FROM actual_sqos
LEFT JOIN sqo_target ON
actual_sqos.sqos_month=sqo_target.kpi_month
AND actual_sqos.region=sqo_target.kpi_region
WHERE region IS NOT null