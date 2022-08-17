

  create  table "acton"."dbt_actonmarketing"."avg_lead_to_cw_cohort__dbt_tmp"
  as (
    WITH base AS(

    SELECT *
    FROM "acton"."dbt_actonmarketing"."lead_to_cw_cohort"
)

SELECT
    channel_bucket,
    segment,
    AVG(days_to_mql) AS avg_days_to_mql,
    AVG(days_to_sal) AS avg_days_to_sal,
    AVG(days_to_sql) AS avg_days_to_sql,
    AVG(days_to_sqo) AS avg_days_to_sqo,
    AVG(days_to_won) AS avg_days_to_won,
    AVG(days_to_closed_lost) AS avg_days_to_closed_lost
FROM base
GROUP BY 1,2
  );