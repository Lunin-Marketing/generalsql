

  create  table "acton"."dbt_actonmarketing"."kpi_targets__dbt_tmp"
  as (
    

WITH base AS (

SELECT *
FROM "acton".public."kpi_targets"

)

SELECT *
FROM base
  );