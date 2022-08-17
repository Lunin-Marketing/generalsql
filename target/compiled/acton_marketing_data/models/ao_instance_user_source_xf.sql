

WITH base AS (

SELECT *
FROM "acton".salesforce."act_on_instance_user_c"

), final AS (
    SELECT
        base.id AS ao_user_id,
        base.is_deleted,
        base.name AS ao_user_name,
        base.created_date AS ao_user_created_date,
        base.last_modified_date,
        base.system_modstamp AS systemmodstamp,
        base.act_on_instance_c AS ao_user_instance,
        base.email_c AS ao_user_email, 
        base.first_name_c AS ao_user_first_name, 
        base.last_name_c AS ao_user_last_name, 
        base.is_admin_user_c AS is_admin_user,
        base.is_marketing_user_c AS is_marketing_user,
        base.is_sales_user_c AS is_sales_user,
        base.date_user_created_c AS ao_user_date_created,
        base.ao_user_account_id_c AS ao_user_account_id, 
        base.contact_c AS ao_user_contact_id,
        base.deleted_c AS deleted,
        base.deleted_date_c AS deleted_date,
        ao_instance_source_xf.ao_instance_name AS ao_user_account_name
    FROM base
    LEFT JOIN "acton"."dbt_actonmarketing"."ao_instance_source_xf" ON
    base.act_on_instance_c=ao_instance_source_xf.ao_instance_id
    
)

SELECT *
FROM final