{{ config(materialized='table') }}

WITH kpi_base AS (

    SELECT
        DATE_TRUNC('Month',created_date) AS kpi_month,
        COUNT(DISTINCT lead_id) AS kpi
    FROM {{ref('funnel_report_all_time_leads')}}
    WHERE DATE_TRUNC('Month',created_date)=DATE_TRUNC('Month',CURRENT_DATE)
    GROUP BY 1

), kpi_target AS (

    SELECT
        kpi_month,
        kpi_target
    FROM {{ref('kpi_targets')}}
    WHERE kpi_month=DATE_TRUNC('Month',CURRENT_DATE)
    AND kpi_lead_source = 'total'
    AND kpi = 'target_leads'

), final AS (

    SELECT
        kpi,
        kpi_target,
        kpi/kpi_target AS pct_to_target
    FROM kpi_base
    LEFT JOIN kpi_target ON
    kpi_base.kpi_month=kpi_target.kpi_month
)

SELECT *
FROM final