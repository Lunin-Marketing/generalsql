

  create  table "acton"."dbt_actonmarketing"."wow_weektracker__dbt_tmp"
  as (
    

WITH last_week AS (
SELECT
week 
FROM "acton"."dbt_actonmarketing"."date_base_xf"
--FROM "acton".dbt_actonmarketing.date_base_xf
WHERE day=CURRENT_DATE-7

)

SELECT
week::Date AS last_week_start,
week::Date +6 AS last_week_end
FROM last_week
  );