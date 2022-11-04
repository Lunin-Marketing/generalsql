

  create  table "acton"."dbt_actonmarketing"."funnel_to_target_lead2mql__dbt_tmp"
  as (
    

WITH lead_kpi_base AS (

    SELECT
        'FY22-Q4' AS kpi_month,
        COUNT(DISTINCT lead_id) AS kpi
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_leads"
    WHERE DATE_TRUNC('Month',created_date) IN ('2022-12-01','2022-11-01','2022-10-01')
    GROUP BY 1

), mql_kpi_base AS (

    SELECT
        'FY22-Q4' AS kpi_month,
        COUNT(DISTINCT mql_id) AS kpi
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_mqls"
    WHERE DATE_TRUNC('Month',mql_date) IN ('2022-12-01','2022-11-01','2022-10-01')
    GROUP BY 1

), kpi_target AS (

    SELECT
        'FY22-Q4' AS kpi_month,
        AVG(kpi_target) AS kpi_target
    FROM "acton"."dbt_actonmarketing"."kpi_targets"
    WHERE kpi_month IN ('2022-12-01','2022-11-01','2022-10-01')
    AND kpi_lead_source = 'total'
    AND kpi = 'target_l2m_conv'

), intermediate AS (

    SELECT
        lead_kpi_base.kpi AS leads,
        lead_kpi_base.kpi_month,
        mql_kpi_base.kpi AS mqls,
        SUM(mql_kpi_base.kpi)/SUM(lead_kpi_base.kpi) AS conversion_kpi
    FROM lead_kpi_base
    LEFT JOIN mql_kpi_base ON
    lead_kpi_base.kpi_month=mql_kpi_base.kpi_month
    GROUP BY 1,2,3

), final AS (

    SELECT
        conversion_kpi AS kpi,
        kpi_target,
        'Lead to MQL' AS key_metric
       -- kpi/kpi_target AS pct_to_target
    FROM intermediate
    LEFT JOIN kpi_target ON
    intermediate.kpi_month=kpi_target.kpi_month
    --GROUP BY 1,2
)

SELECT *
FROM final
  );