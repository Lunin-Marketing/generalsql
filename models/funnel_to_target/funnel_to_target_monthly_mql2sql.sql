{{ config(materialized='table') }}

WITH sql_kpi_base AS (

    SELECT
        DATE_TRUNC('month',sql_date)::Date AS kpi_month,
        COUNT(DISTINCT sql_id) AS kpi
    FROM {{ref('funnel_report_all_time_sqls')}}
    WHERE opp_type = 'New Business'
    GROUP BY 1

), mql_kpi_base AS (

    SELECT
        DATE_TRUNC('month',mql_date)::Date AS kpi_month,
        COUNT(DISTINCT mql_id) AS kpi
    FROM {{ref('funnel_report_all_time_mqls')}}
    WHERE is_current_customer = false
    GROUP BY 1

), kpi_target AS (

    SELECT
        kpi_month,
        SUM(kpi_target) AS kpi_target
    FROM {{ref('kpi_targets')}}
    WHERE kpi_lead_source LIKE '%total%'
    AND kpi LIKE '%target_m2sql_conv%'
    GROUP BY 1
    UNION ALL 
    SELECT
        kpi_month,
        SUM(kpi_target) AS kpi_target
    FROM {{ref('kpi_targets_2022')}}
    WHERE kpi_lead_source LIKE '%total%'
    AND kpi LIKE '%target_m2sql_conv%'
    GROUP BY 1

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
        intermediate.kpi_month,
        conversion_kpi AS kpi,
        kpi_target,
        'MQL to SQL' AS key_metric
    FROM intermediate
    LEFT JOIN kpi_target ON
    intermediate.kpi_month=kpi_target.kpi_month

)

SELECT *
FROM final
WHERE kpi_target IS NOT null
ORDER BY 1 DESC