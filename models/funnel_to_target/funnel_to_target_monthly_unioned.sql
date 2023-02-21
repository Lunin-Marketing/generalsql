{{ config(materialized='table') }}

WITH base AS (
    SELECT
        kpi_month,
        kpi,
        kpi_target,
        key_metric
    FROM {{ref('funnel_to_target_monthly_leads')}}
    UNION ALL
    SELECT
        kpi_month,
        kpi,
        kpi_target,
        key_metric
    FROM {{ref('funnel_to_target_monthly_asp')}}
    UNION ALL
    SELECT
        kpi_month,
        kpi,
        kpi_target,
        key_metric
    FROM {{ref('funnel_to_target_monthly_lead2mql')}}
    UNION ALL
    SELECT
        kpi_month,
        kpi,
        kpi_target,
        key_metric
    FROM {{ref('funnel_to_target_monthly_mql2sql')}}
    UNION ALL
    SELECT
        kpi_month,
        kpi,
        kpi_target,
        key_metric
    FROM {{ref('funnel_to_target_monthly_mqls')}}
    UNION ALL
    SELECT
        kpi_month,
        kpi,
        kpi_target,
        key_metric
    FROM {{ref('funnel_to_target_monthly_sql2sqo')}}
    UNION ALL
    SELECT
        kpi_month,
        kpi,
        kpi_target,
        key_metric
    FROM {{ref('funnel_to_target_monthly_sqls')}}
    UNION ALL
    SELECT
        kpi_month,
        kpi,
        kpi_target,
        key_metric
    FROM {{ref('funnel_to_target_monthly_sqo_pipeline')}}
    UNION ALL
    SELECT
        kpi_month,
        kpi,
        kpi_target,
        key_metric
    FROM {{ref('funnel_to_target_monthly_sqos')}}

)

SELECT *
FROM base