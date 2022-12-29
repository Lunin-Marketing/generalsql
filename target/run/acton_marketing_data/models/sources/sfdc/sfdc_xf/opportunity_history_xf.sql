
  
    

        create or replace transient table AO_MARKETING.dbt_snowflake.opportunity_history_xf  as
        (

WITH base AS (

SELECT *
FROM AO_MARKETING.salesforce.opportunity_field_history

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
      
  