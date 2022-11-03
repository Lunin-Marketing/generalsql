

WITH sql_kpi_base AS (

    SELECT
        DATE_TRUNC('Month',sql_date) AS kpi_month,
        COUNT(DISTINCT sql_id) AS kpi
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_sqls"
    WHERE DATE_TRUNC('Month',sql_date)=DATE_TRUNC('Month',CURRENT_DATE)
    GROUP BY 1

), sqo_kpi_base AS (

    SELECT
        DATE_TRUNC('Month',sqo_date) AS kpi_month,
        COUNT(DISTINCT sqo_id) AS kpi
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_sqos"
    WHERE DATE_TRUNC('Month',sqo_date)=DATE_TRUNC('Month',CURRENT_DATE)
    GROUP BY 1

), kpi_target AS (

    SELECT
        kpi_month,
        kpi_target::DECIMAL
    FROM "acton"."dbt_actonmarketing"."kpi_targets"
    WHERE kpi_month=DATE_TRUNC('Month',CURRENT_DATE)
    AND kpi_lead_source = 'total'
    AND kpi = 'target_sql2sqo_conv'

), intermediate AS (

    SELECT
        sql_kpi_base.kpi AS sqls,
        sql_kpi_base.kpi_month,
        sqo_kpi_base.kpi AS sqos,
        SUM(sqo_kpi_base.kpi)/SUM(sql_kpi_base.kpi) AS conversion_kpi
    FROM sql_kpi_base
    LEFT JOIN sqo_kpi_base ON
    sql_kpi_base.kpi_month=sqo_kpi_base.kpi_month
    GROUP BY 1,2,3

), final AS (

    SELECT
        conversion_kpi AS sql2sqo,
        kpi_target AS sql2sqo_target,
        conversion_kpi/kpi_target AS pct_to_target
    FROM intermediate
    LEFT JOIN kpi_target ON
    intermediate.kpi_month=kpi_target.kpi_month
    --GROUP BY 1,2
)

SELECT *
FROM final