

WITH asp_target AS (

    SELECT 
        kpi_month,
        kpi_region,
        kpi_target AS asp_target
    FROM "acton"."dbt_actonmarketing"."kpi_targets"
    WHERE kpi = 'target_asp'

), actual_asp AS (

    SELECT
        DATE_TRUNC('month',sqo_date)::Date AS asp_month,
        CASE 
            WHEN account_global_region LIKE 'NA-%' THEN 'NA'
            WHEN account_global_region IN ('EUROPE') THEN 'EMEA'
            ELSE null
        END AS region,
        SUM(acv)/COUNT(DISTINCT sqo_id) AS asp
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_sqos"
    WHERE sqo_date >= '2022-04-01'
    GROUP BY 1,2
)

SELECT
    asp_month,
    actual_asp.region,
    asp,
    asp_target,
    asp/asp_target AS actual_vs_target
FROM actual_asp
LEFT JOIN asp_target ON
actual_asp.asp_month=asp_target.kpi_month
AND actual_asp.region=asp_target.kpi_region
WHERE region IS NOT null