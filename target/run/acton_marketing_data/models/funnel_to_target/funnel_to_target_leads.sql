

  create  table "acton"."dbt_actonmarketing"."funnel_to_target_leads__dbt_tmp"
  as (
    

WITH kpi_base AS (

    SELECT
        'FY22-Q4' AS kpi_month,
        COUNT(DISTINCT lead_id) AS kpi
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_leads"
    WHERE DATE_TRUNC('Month',created_date) IN ('2022-12-01','2022-11-01','2022-10-01')
    AND is_current_customer = false
    GROUP BY 1

), kpi_target AS (

    SELECT
        'FY22-Q4' AS kpi_month,
        SUM(kpi_target) AS kpi_target
    FROM "acton"."dbt_actonmarketing"."kpi_targets"
    WHERE kpi_month IN ('2022-12-01','2022-11-01','2022-10-01')
    AND kpi_lead_source = 'total'
    AND kpi = 'target_leads'

), final AS (

    SELECT
        kpi,
        kpi_target,
        'Leads' AS key_metric
       -- kpi/kpi_target AS pct_to_target
    FROM kpi_base
    LEFT JOIN kpi_target ON
    kpi_base.kpi_month=kpi_target.kpi_month
)

SELECT *
FROM final
  );