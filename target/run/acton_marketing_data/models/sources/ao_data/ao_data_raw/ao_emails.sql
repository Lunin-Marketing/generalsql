

  create  table "acton"."dbt_actonmarketing"."ao_emails__dbt_tmp"
  as (
    

WITH base AS (

    SELECT *
    FROM "acton"."data_studio_s3"."data_studio_emails"

)

SELECT *
FROM base
  );