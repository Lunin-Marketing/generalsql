{{ config(materialized='table') }}

WITH kpi_base AS (

    SELECT
        DATE_TRUNC('Month',sqo_date) AS kpi_month,
        SUM(acv) AS kpi
    FROM {{ref('funnel_report_all_time_pipeline')}}
    WHERE DATE_TRUNC('Month',sqo_date)=DATE_TRUNC('Month',CURRENT_DATE)
    GROUP BY 1

), kpi_target AS (

    SELECT
        kpi_month,
        kpi_target
    FROM {{ref('kpi_targets')}}
    WHERE kpi_month=DATE_TRUNC('Month',CURRENT_DATE)
    AND kpi_lead_source = 'total'
    AND kpi = 'target_sqo_pipe'

), final AS (

    SELECT
        kpi AS pipeline,
        kpi_target AS pipeline_target,
        kpi/kpi_target AS pct_to_target
    FROM kpi_base
    LEFT JOIN kpi_target ON
    kpi_base.kpi_month=kpi_target.kpi_month
)

SELECT *
FROM final