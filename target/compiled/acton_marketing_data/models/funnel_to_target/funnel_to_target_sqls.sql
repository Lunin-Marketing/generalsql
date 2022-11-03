

WITH kpi_base AS (

    SELECT
        DATE_TRUNC('Month',sql_date) AS kpi_month,
        COUNT(DISTINCT sql_id) AS kpi
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_sqls"
    WHERE DATE_TRUNC('Month',sql_date)=DATE_TRUNC('Month',CURRENT_DATE)
    GROUP BY 1

), kpi_target AS (

    SELECT
        kpi_month,
        kpi_target
    FROM "acton"."dbt_actonmarketing"."kpi_targets"
    WHERE kpi_month=DATE_TRUNC('Month',CURRENT_DATE)
    AND kpi_lead_source = 'total'
    AND kpi = 'target_sqls'

), final AS (

    SELECT
        kpi AS sqls,
        kpi_target AS sqls_target,
        kpi/kpi_target AS pct_to_target
    FROM kpi_base
    LEFT JOIN kpi_target ON
    kpi_base.kpi_month=kpi_target.kpi_month
)

SELECT *
FROM final