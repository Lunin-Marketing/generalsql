

  create  table "acton"."dbt_actonmarketing"."hard_bounce_customer__dbt_tmp"
  as (
    

SELECT
    1 AS person_account_id
FROM "acton"."dbt_actonmarketing"."person_source_xf"
  );