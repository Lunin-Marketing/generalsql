{{ config(materialized='table') }}

WITH sql_kpi_base AS (

    SELECT
        'FY23-Q1' AS kpi_month,
        COUNT(DISTINCT sql_id) AS kpi
    FROM {{ref('funnel_report_all_time_sqls')}}
    WHERE DATE_TRUNC('Month',sql_date) IN ('2023-01-01','2023-02-01','2023-03-01')
    AND opp_type = 'New Business'
    GROUP BY 1

), mql_kpi_base AS (

    SELECT
        'FY23-Q1' AS kpi_month,
        COUNT(DISTINCT mql_id) AS kpi
    FROM {{ref('funnel_report_all_time_mqls')}}
    WHERE DATE_TRUNC('Month',mql_date) IN ('2023-01-01','2023-02-01','2023-03-01')
    AND is_current_customer = false
    GROUP BY 1

), kpi_target AS (

    SELECT
        'FY23-Q1' AS kpi_month,
        AVG(kpi_target) AS kpi_target
    FROM {{ref('kpi_targets')}}
    WHERE kpi_month IN ('2023-01-01','2023-02-01','2023-03-01')
    AND kpi_lead_source LIKE '%total%'
    AND kpi LIKE '%target_m2sql_conv%'

), intermediate AS (

    SELECT
        sql_kpi_base.kpi AS sqls,
        mql_kpi_base.kpi_month,
        mql_kpi_base.kpi AS mqls,
        SUM(sql_kpi_base.kpi)/SUM(mql_kpi_base.kpi) AS conversion_kpi
    FROM mql_kpi_base
    LEFT JOIN sql_kpi_base ON
    mql_kpi_base.kpi_month=sql_kpi_base.kpi_month
    GROUP BY 1,2,3

), final AS (

    SELECT
        conversion_kpi AS kpi,
        kpi_target,
        'MQL to SQL' AS key_metric
       -- kpi/kpi_target AS pct_to_target
    FROM intermediate
    LEFT JOIN kpi_target ON
    intermediate.kpi_month=kpi_target.kpi_month
    --GROUP BY 1,2
)

SELECT *
FROM final