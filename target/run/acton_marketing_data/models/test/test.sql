

  create  table "acton"."dbt_actonmarketing"."test__dbt_tmp"
  as (
    

SELECT DISTINCT
   person_id
FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_cohort"
WHERE mql_created_date >= '2022-08-01'
AND mql_created_date <= '2022-09-30'
  );