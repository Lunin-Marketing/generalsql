{{ config(materialized='table') }}

WITH m2sql_target AS (

    SELECT 
        kpi_month,
        kpi_region,
        kpi_target AS m2sql_target
    FROM {{ref('kpi_targets')}}
    WHERE kpi = 'target_m2sql_conv'

), actual_sqls AS (

    SELECT
        DATE_TRUNC('month',sql_date)::Date AS sqls_month,
        CASE 
            WHEN account_global_region LIKE 'NA-%' THEN 'NA'
            WHEN account_global_region IN ('EUROPE') THEN 'EMEA'
            ELSE null
        END AS region,
        COUNT(DISTINCT sql_id) AS sqls
    FROM {{ref('funnel_report_all_time_sqls')}}
    WHERE sql_date >= '2022-04-01'
    GROUP BY 1,2

), actual_mqls AS (

    SELECT
        DATE_TRUNC('month',mql_date)::Date AS mqls_month,
        CASE 
            WHEN global_region LIKE 'NA-%' THEN 'NA'
            WHEN global_region IN ('EUROPE') THEN 'EMEA'
            ELSE null
        END AS region,
        COUNT(DISTINCT mql_id) AS mqls
    FROM {{ref('funnel_report_all_time_mqls')}}
    WHERE mql_date >= '2022-04-01'
    GROUP BY 1,2

), actual_m2sql AS (

    SELECT
        sqls_month,
        actual_sqls.region,
        SUM(sqls)/SUM(mqls) AS m2sql_actual
    FROM actual_sqls
    LEFT JOIN actual_mqls ON 
    actual_sqls.region=actual_mqls.region
    AND actual_sqls.sqls_month=actual_mqls.mqls_month
    WHERE actual_sqls.region IS NOT null
    GROUP BY 1,2

)

SELECT
    sqls_month,
    actual_m2sql.region,
    m2sql_actual,
    m2sql_target,
    m2sql_actual/m2sql_target AS actual_vs_target
FROM actual_m2sql
LEFT JOIN m2sql_target ON
actual_m2sql.sqls_month=m2sql_target.kpi_month
AND actual_m2sql.region=m2sql_target.kpi_region
WHERE region IS NOT null