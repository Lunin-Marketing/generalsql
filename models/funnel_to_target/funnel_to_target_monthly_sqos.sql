{{ config(materialized='table') }}

WITH kpi_base AS (

    SELECT
        DATE_TRUNC('month',sqo_date)::Date AS kpi_month,
        COUNT(DISTINCT sqo_id) AS kpi
    FROM {{ref('funnel_report_all_time_sqos')}}
    WHERE opp_type = 'New Business'
    GROUP BY 1

), kpi_target AS (

    SELECT
        kpi_month,
        SUM(kpi_target) AS kpi_target
    FROM {{ref('kpi_targets')}}
    WHERE kpi_lead_source LIKE '%total%'
    AND kpi LIKE '%target_sqos%'
    GROUP BY 1
    UNION ALL 
    SELECT
        kpi_month,
        SUM(kpi_target) AS kpi_target
    FROM {{ref('kpi_targets_2022')}}
    WHERE kpi_lead_source LIKE '%total%'
    AND kpi LIKE '%target_sqos%'
    GROUP BY 1

), final AS (

    SELECT
        kpi_base.kpi_month,
        kpi,
        kpi_target,
        'SQOs' AS key_metric
    FROM kpi_base
    LEFT JOIN kpi_target ON
    kpi_base.kpi_month=kpi_target.kpi_month
)

SELECT *
FROM final
WHERE kpi_target IS NOT null
ORDER BY 1 DESC