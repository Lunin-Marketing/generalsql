{{ config(materialized='table') }}

WITH kpi_base AS (

    SELECT
        'FY23-Q1' AS kpi_month,
        COUNT(DISTINCT sqo_id) AS kpi
    FROM {{ref('funnel_report_all_time_sqos')}}
    WHERE DATE_TRUNC('Month',sqo_date) IN ('2023-01-01','2023-02-01','2023-03-01')
    AND opp_type = 'New Business'
    GROUP BY 1

), kpi_target AS (

    SELECT
        'FY23-Q1' AS kpi_month,
        SUM(kpi_target) AS kpi_target
    FROM {{ref('kpi_targets')}}
    WHERE kpi_month IN ('2023-01-01','2023-02-01','2023-03-01')
    AND kpi_lead_source = 'total'
    AND kpi = 'target_sqos'

), final AS (

    SELECT
        kpi,
        kpi_target,
        'SQOs' AS key_metric
       -- kpi/kpi_target AS pct_to_target
    FROM kpi_base
    LEFT JOIN kpi_target ON
    kpi_base.kpi_month=kpi_target.kpi_month
)

SELECT *
FROM final