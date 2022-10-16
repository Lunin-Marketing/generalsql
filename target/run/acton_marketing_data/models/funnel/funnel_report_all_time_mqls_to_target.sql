

  create  table "acton"."dbt_actonmarketing"."funnel_report_all_time_mqls_to_target__dbt_tmp"
  as (
    

WITH mql_target AS (

    SELECT 
        kpi_month,
        kpi_region,
        kpi_target AS mqls_target
    FROM "acton"."dbt_actonmarketing"."kpi_targets"
    WHERE kpi = 'target_mqls'

), actual_mqls AS (

    SELECT
        DATE_TRUNC('month',mql_date)::Date AS mqls_month,
        CASE 
            WHEN global_region LIKE 'NA-%' THEN 'NA'
            WHEN global_region IN ('EUROPE') THEN 'EMEA'
            ELSE null
        END AS region,
        COUNT(DISTINCT mql_id) AS mqls
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_mqls"
    WHERE mql_date >= '2022-04-01'
    GROUP BY 1,2
)

SELECT
    mqls_month,
    actual_mqls.region,
    mqls,
    mqls_target,
    mqls/mqls_target AS actual_vs_target
FROM actual_mqls
LEFT JOIN mql_target ON
actual_mqls.mqls_month=mql_target.kpi_month
AND actual_mqls.region=mql_target.kpi_region
WHERE region IS NOT null
  );