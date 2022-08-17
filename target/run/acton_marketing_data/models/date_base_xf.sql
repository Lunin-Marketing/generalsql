

  create  table "acton"."dbt_actonmarketing"."date_base_xf__dbt_tmp"
  as (
    
SELECT
day::date AS day,
week,
month,
month_name,
quarter,
fy
FROM "acton".public.date_base
  );