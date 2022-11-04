

WITH base AS (
    SELECT
        kpi,
        kpi_target,
        key_metric
    FROM "acton"."dbt_actonmarketing"."funnel_to_target_leads"
    UNION ALL
    SELECT
        kpi,
        kpi_target,
        key_metric
    FROM "acton"."dbt_actonmarketing"."funnel_to_target_asp"
    UNION ALL
    SELECT
        kpi,
        kpi_target,
        key_metric
    FROM "acton"."dbt_actonmarketing"."funnel_to_target_lead2mql"
    UNION ALL
    SELECT
        kpi,
        kpi_target,
        key_metric
    FROM "acton"."dbt_actonmarketing"."funnel_to_target_mql2sql"
    UNION ALL
    SELECT
        kpi,
        kpi_target,
        key_metric
    FROM "acton"."dbt_actonmarketing"."funnel_to_target_mqls"
    UNION ALL
    SELECT
        kpi,
        kpi_target,
        key_metric
    FROM "acton"."dbt_actonmarketing"."funnel_to_target_sql2sqo"
    UNION ALL
    SELECT
        kpi,
        kpi_target,
        key_metric
    FROM "acton"."dbt_actonmarketing"."funnel_to_target_sqls"
    UNION ALL
    SELECT
        kpi,
        kpi_target,
        key_metric
    FROM "acton"."dbt_actonmarketing"."funnel_to_target_sqo_pipe"
    UNION ALL
    SELECT
        kpi,
        kpi_target,
        key_metric
    FROM "acton"."dbt_actonmarketing"."funnel_to_target_sqos"

)

SELECT *
FROM base