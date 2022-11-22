

  create  table "acton"."dbt_actonmarketing"."test3__dbt_tmp"
  as (
    

SELECT
    person_id,
    industry,
    industry_bucket
FROM "acton"."dbt_actonmarketing"."person_source_xf"
  );