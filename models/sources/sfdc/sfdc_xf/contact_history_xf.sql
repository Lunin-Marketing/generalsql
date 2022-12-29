{{ config(materialized='table') }}

WITH base AS (

SELECT *
FROM {{ source('aws_salesforce', 'contact_history') }}

)

SELECT
id AS contact_history_id, 
contact_id,
created_date AS field_modified_at,
field,
old_value,
new_value
FROM base
WHERE is_deleted = false