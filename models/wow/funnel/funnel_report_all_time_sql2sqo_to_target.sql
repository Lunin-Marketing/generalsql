{{ config(materialized='table') }}

WITH sql2sqo_target AS (

    SELECT 
        kpi_month,
        kpi_region,
        kpi_target AS sql2sqo_target
    FROM {{ref('kpi_targets')}}
    WHERE kpi = 'target_sql2sqo_conv'

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

), actual_sqos AS (

    SELECT
        DATE_TRUNC('month',sqo_date)::Date AS sqos_month,
        CASE 
            WHEN account_global_region LIKE 'NA-%' THEN 'NA'
            WHEN account_global_region IN ('EUROPE') THEN 'EMEA'
            ELSE null
        END AS region,
        COUNT(DISTINCT sqo_id) AS sqos
    FROM {{ref('funnel_report_all_time_sqos')}}
    WHERE sqo_date >= '2022-04-01'
    GROUP BY 1,2

), actual_sql2sqo AS (

    SELECT
        sqos_month,
        actual_sqos.region,
        SUM(sqos)/SUM(sqls) AS sql2sqo_actual
    FROM actual_sqos
    LEFT JOIN actual_sqls ON 
    actual_sqos.region=actual_sqls.region
    AND actual_sqos.sqos_month=actual_sqls.sqls_month
    WHERE actual_sqos.region IS NOT null
    GROUP BY 1,2

)

SELECT
    sqos_month,
    actual_sql2sqo.region,
    sql2sqo_actual,
    sql2sqo_target,
    sql2sqo_actual/sql2sqo_target AS actual_vs_target
FROM actual_sql2sqo
LEFT JOIN sql2sqo_target ON
actual_sql2sqo.sqos_month=sql2sqo_target.kpi_month
AND actual_sql2sqo.region=sql2sqo_target.kpi_region
WHERE region IS NOT null