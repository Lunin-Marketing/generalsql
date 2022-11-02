

  create  table "acton"."dbt_actonmarketing"."funnel_to_target_leads__dbt_tmp"
  as (
    

WITH kpi_base AS (

    SELECT
        DATE_TRUNC('Month',marketing_created_date) AS kpi_month,
        COUNT(DISTINCT lead_id) AS kpi
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_leads"
    WHERE DATE_TRUNC('Month',marketing_created_date)=DATE_TRUNC('Month',CURRENT_DATE)
    GROUP BY 1

), kpi_target AS (

    SELECT
        kpi_month,
        kpi_target
    FROM "acton"."dbt_actonmarketing"."kpi_targets"
    WHERE kpi_month=DATE_TRUNC('Month',CURRENT_DATE)
    AND kpi_lead_source = 'total'
    AND kpi = 'target_leads'

), final AS (

    SELECT
        kpi,
        kpi_target,
        kpi/kpi_target AS pct_to_target
    FROM kpi_base
    LEFT JOIN kpi_target ON
    kpi_base.kpi_month=kpi_target.kpi_month
)

SELECT *
FROM final
  );