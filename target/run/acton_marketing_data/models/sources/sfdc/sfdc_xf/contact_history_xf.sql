
  
    

        create or replace transient table AO_MARKETING.dbt_snowflake.contact_history_xf  as
        (

WITH base AS (

SELECT *
FROM AO_MARKETING.salesforce.contact_history

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
        );
      
  