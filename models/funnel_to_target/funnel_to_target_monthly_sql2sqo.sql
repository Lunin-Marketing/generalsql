{{ config(materialized='table') }}

WITH sql_kpi_base AS (

    SELECT
        DATE_TRUNC('month',sql_date)::Date AS kpi_month,
        COUNT(DISTINCT sql_id) AS kpi
    FROM {{ref('funnel_report_all_time_sqls')}}
    WHERE opp_type = 'New Business'
    GROUP BY 1

), sqo_kpi_base AS (

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
    AND kpi LIKE '%target_sql2sqo_conv%'
    GROUP BY 1
    UNION ALL 
    SELECT
        kpi_month,
        SUM(kpi_target) AS kpi_target
    FROM {{ref('kpi_targets_2022')}}
    WHERE kpi_lead_source LIKE '%total%'
    AND kpi LIKE '%target_sql2sqo_conv%'
    GROUP BY 1

), intermediate AS (

    SELECT
        sql_kpi_base.kpi AS sqls,
        sql_kpi_base.kpi_month,
        sqo_kpi_base.kpi AS sqos,
        SUM(sqo_kpi_base.kpi)/SUM(sql_kpi_base.kpi) AS conversion_kpi
    FROM sql_kpi_base
    LEFT JOIN sqo_kpi_base ON
    sql_kpi_base.kpi_month=sqo_kpi_base.kpi_month
    GROUP BY 1,2,3

), final AS (

    SELECT
        intermediate.kpi_month,
        conversion_kpi AS kpi,
        kpi_target,
        'SQL to SQO' AS key_metric
    FROM intermediate
    LEFT JOIN kpi_target ON
    intermediate.kpi_month=kpi_target.kpi_month
    
)

SELECT *
FROM final
WHERE kpi_target IS NOT null
ORDER BY 1 DESC