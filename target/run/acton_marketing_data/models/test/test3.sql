

  create  table "acton"."dbt_actonmarketing"."test3__dbt_tmp"
  as (
    

SELECT
    sqo_id,
    acv
FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_pipeline"
WHERE created_date >= '2022-10-01'
AND created_date <= '2022-12-31'
AND sqo_date >= '2022-10-01'
AND sqo_date <= '2022-12-31'
AND opp_type = 'New Business'
  );