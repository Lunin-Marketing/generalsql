

  create  table "acton"."dbt_actonmarketing"."avg_lead_to_cw_cohort_count__dbt_tmp"
  as (
    WITH base AS(

    SELECT *
    FROM "acton"."dbt_actonmarketing"."lead_to_cw_cohort"
)

SELECT DISTINCT
    channel_bucket,
    segment,
    SUM(is_mql) AS mqls,
    SUM(is_sal) AS sals,
    SUM(is_sql) AS sqls,
    SUM(is_sqo) AS sqos,
    SUM(is_cl) AS closed_lost,
    SUM(is_cw) AS closed_won
FROM base
GROUP BY 1,2
  );