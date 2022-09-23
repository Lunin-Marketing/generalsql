

  create  table "acton"."dbt_actonmarketing"."ao_webinars__dbt_tmp"
  as (
    

WITH base AS (

    SELECT *
    FROM "acton"."data_studio_s3"."data_studio_webinars"

)

SELECT *
FROM base
  );