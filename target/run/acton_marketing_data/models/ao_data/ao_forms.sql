

  create  table "acton"."dbt_actonmarketing"."ao_forms__dbt_tmp"
  as (
    

WITH base AS (

    SELECT *
    FROM "acton"."data_studio_s3"."data_studio_forms"

)

SELECT *
FROM base
  );