

WITH sql_kpi_base AS (

    SELECT
        DATE_TRUNC('Month',sql_date) AS kpi_month,
        COUNT(DISTINCT sql_id) AS kpi
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_sqls"
    WHERE DATE_TRUNC('Month',sql_date)=DATE_TRUNC('Month',CURRENT_DATE)
    GROUP BY 1

), mql_kpi_base AS (

    SELECT
        DATE_TRUNC('Month',mql_date) AS kpi_month,
        COUNT(DISTINCT mql_id) AS kpi
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_mqls"
    WHERE DATE_TRUNC('Month',mql_date)=DATE_TRUNC('Month',CURRENT_DATE)
    GROUP BY 1

), kpi_target AS (

    SELECT
        kpi_month,
        kpi_target::DECIMAL
    FROM "acton"."dbt_actonmarketing"."kpi_targets"
    WHERE kpi_month=DATE_TRUNC('Month',CURRENT_DATE)
    AND kpi_lead_source = 'total'
    AND kpi = 'target_m2sql_conv'

), intermediate AS (

    SELECT
        sql_kpi_base.kpi AS sqls,
        mql_kpi_base.kpi_month,
        mql_kpi_base.kpi AS mqls,
        SUM(sql_kpi_base.kpi)/SUM(mql_kpi_base.kpi) AS conversion_kpi
    FROM mql_kpi_base
    LEFT JOIN sql_kpi_base ON
    mql_kpi_base.kpi_month=sql_kpi_base.kpi_month
    GROUP BY 1,2,3

), final AS (

    SELECT
        conversion_kpi AS mql2sql,
        kpi_target AS mql2sql_target,
        conversion_kpi/kpi_target AS pct_to_target
    FROM intermediate
    LEFT JOIN kpi_target ON
    intermediate.kpi_month=kpi_target.kpi_month
    --GROUP BY 1,2
)

SELECT *
FROM final