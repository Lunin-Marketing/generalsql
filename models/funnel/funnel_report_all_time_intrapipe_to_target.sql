{{ config(materialized='table') }}

WITH intrapipe_target AS (

    SELECT 
        kpi_month,
        kpi_region,
        kpi_target AS intrapipe_target
    FROM {{ref('kpi_targets')}}
    WHERE kpi = 'target_intra_qtr_pipe'

), actual_intrapipe AS (

    SELECT
        DATE_TRUNC('month',sqo_date)::Date AS intrapipe_month,
        CASE 
            WHEN account_global_region LIKE 'NA-%' THEN 'NA'
            WHEN account_global_region IN ('EUROPE') THEN 'EMEA'
            ELSE null
        END AS region,
        SUM(acv)/COUNT(DISTINCT sqo_id) AS intrapipe
    FROM {{ref('funnel_report_all_time_sqos')}}
    WHERE sqo_date >= '2022-04-01'
    AND DATE_TRUNC('month',sqo_date)=DATE_TRUNC('month',close_date)
    GROUP BY 1,2
)

SELECT
    intrapipe_month,
    actual_intrapipe.region,
    intrapipe,
    intrapipe_target,
    intrapipe/intrapipe_target AS actual_vs_target
FROM actual_intrapipe
LEFT JOIN intrapipe_target ON
actual_intrapipe.intrapipe_month=intrapipe_target.kpi_month
AND actual_intrapipe.region=intrapipe_target.kpi_region
WHERE region IS NOT null