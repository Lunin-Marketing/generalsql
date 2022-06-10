{{ config(materialized='table') }}

WITH base AS (

SELECT *
FROM "acton".salesforce."opportunity_field_history"

)

SELECT
id AS contact_history_id, 
opportunity_id,
created_date AS field_modified_at,
field,
old_value,
new_value
FROM base
WHERE is_deleted = false