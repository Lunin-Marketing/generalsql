

  create  table "acton"."dbt_actonmarketing"."lead_history_xf__dbt_tmp"
  as (
    

WITH base AS (

SELECT *
FROM "acton".salesforce."lead_history"

)

SELECT
id AS lead_history_id, 
base.lead_id,
base.created_date AS field_modified_at,
field,
old_value,
new_value
FROM base
LEFT JOIN "acton"."dbt_actonmarketing"."lead_source_xf" ON
base.lead_id=lead_source_xf.lead_id
WHERE base.is_deleted = false
AND is_converted=false
  );