

  create  table "acton"."dbt_actonmarketing"."funnel_to_target_sqls__dbt_tmp"
  as (
    

WITH kpi_base AS (

    SELECT
        'FY22-Q4' AS kpi_month,
        COUNT(DISTINCT sql_id) AS kpi
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_sqls"
    WHERE DATE_TRUNC('Month',sql_date) IN ('2022-12-01','2022-11-01','2022-10-01')
    GROUP BY 1

), kpi_target AS (

    SELECT
        'FY22-Q4' AS kpi_month,
        SUM(kpi_target) AS kpi_target
    FROM "acton"."dbt_actonmarketing"."kpi_targets"
    WHERE kpi_month IN ('2022-12-01','2022-11-01','2022-10-01')
    AND kpi_lead_source = 'total'
    AND kpi = 'target_sqls'

), final AS (

    SELECT
        kpi,
        kpi_target,
        'SQLs' AS key_metric
       -- kpi/kpi_target AS pct_to_target
    FROM kpi_base
    LEFT JOIN kpi_target ON
    kpi_base.kpi_month=kpi_target.kpi_month
)

SELECT *
FROM final
  );