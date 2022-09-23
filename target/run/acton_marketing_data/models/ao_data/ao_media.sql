

  create  table "acton"."dbt_actonmarketing"."ao_media__dbt_tmp"
  as (
    

WITH base AS (

    SELECT *
    FROM "acton"."data_studio_s3"."data_studio_media"

)

SELECT *
FROM base
  );