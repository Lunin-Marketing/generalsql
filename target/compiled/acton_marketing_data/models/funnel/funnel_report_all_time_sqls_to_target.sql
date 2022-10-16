

WITH sql_target AS (

    SELECT 
        kpi_month,
        kpi_region,
        kpi_target AS sqls_target
    FROM "acton"."dbt_actonmarketing"."kpi_targets"
    WHERE kpi = 'target_sqls'

), actual_sqls AS (

    SELECT
        DATE_TRUNC('month',sql_date)::Date AS sqls_month,
        CASE 
            WHEN account_global_region LIKE 'NA-%' THEN 'NA'
            WHEN account_global_region IN ('EUROPE') THEN 'EMEA'
            ELSE null
        END AS region,
        COUNT(DISTINCT sql_id) AS sqls
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_sqls"
    WHERE sql_date >= '2022-04-01'
    GROUP BY 1,2
)

SELECT
    sqls_month,
    actual_sqls.region,
    sqls,
    sqls_target,
    sqls/sqls_target AS actual_vs_target
FROM actual_sqls
LEFT JOIN sql_target ON
actual_sqls.sqls_month=sql_target.kpi_month
AND actual_sqls.region=sql_target.kpi_region
WHERE region IS NOT null