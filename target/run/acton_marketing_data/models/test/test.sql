

  create  table "acton"."dbt_actonmarketing"."test__dbt_tmp"
  as (
    WITH base AS (

SELECT *
FROM "acton"."salesforce"."lead"

), intermediate AS (

SELECT
    created_date::Timestamp AS created_date
FROM base
WHERE email ='100443596@alumnos.uc3m.es'

)

SELECT
    cast(created_date at time zone 'UTC' at time zone 'America/Los_Angeles' as TIMESTAMP) As create_date
FROM intermediate
  );