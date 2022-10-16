

  create  table "acton"."dbt_actonmarketing"."opportunity_history_xf__dbt_tmp"
  as (
    

WITH base AS (

SELECT *
FROM "acton"."salesforce"."opportunity_field_history"

)

SELECT
id AS opportunity_history_id, 
opportunity_id,
created_date AS field_modified_at,
field,
old_value,
new_value
FROM base
WHERE is_deleted = false
  );