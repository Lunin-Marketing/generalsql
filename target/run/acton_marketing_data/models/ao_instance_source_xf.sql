

  create  table "acton"."dbt_actonmarketing"."ao_instance_source_xf__dbt_tmp"
  as (
    
WITH base AS (
SELECT *
FROM "acton".salesforce."act_on_instance_c"

), final AS (
    
    SELECT
        id AS ao_instance_id,
        is_deleted,
        name AS ao_instance_name,
        created_date AS ao_instance_created_date,
        last_modified_date,
        system_modstamp AS systemmodstamp,
        ao_account_name_c AS ao_account_name,
        cdb_ao_instance_id_c AS cdb_ao_instance_id,
        account_c AS ao_instance_account,
        parent_account_c AS ao_instance_parent_account,
        ao_parent_id_c AS ao_instance_parent_account_id 
    FROM base
)

SELECT *
FROM final
  );