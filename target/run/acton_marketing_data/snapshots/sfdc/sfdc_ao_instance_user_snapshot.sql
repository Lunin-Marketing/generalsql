
      update "acton"."snapshots"."sfdc_ao_instance_user_snapshot2"
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "sfdc_ao_instance_user_snapshot2__dbt_tmp160523316939" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = "acton"."snapshots"."sfdc_ao_instance_user_snapshot2".dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      and "acton"."snapshots"."sfdc_ao_instance_user_snapshot2".dbt_valid_to is null;

    insert into "acton"."snapshots"."sfdc_ao_instance_user_snapshot2" ("ao_user_id", "is_deleted", "ao_user_name", "ao_user_created_date", "last_modified_date", "systemmodstamp", "ao_user_instance", "ao_user_email", "ao_user_first_name", "ao_user_last_name", "is_admin_user", "is_marketing_user", "is_sales_user", "ao_user_date_created", "ao_user_account_id", "ao_user_contact_id", "deleted", "deleted_date", "ao_user_account_name", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."ao_user_id",DBT_INTERNAL_SOURCE."is_deleted",DBT_INTERNAL_SOURCE."ao_user_name",DBT_INTERNAL_SOURCE."ao_user_created_date",DBT_INTERNAL_SOURCE."last_modified_date",DBT_INTERNAL_SOURCE."systemmodstamp",DBT_INTERNAL_SOURCE."ao_user_instance",DBT_INTERNAL_SOURCE."ao_user_email",DBT_INTERNAL_SOURCE."ao_user_first_name",DBT_INTERNAL_SOURCE."ao_user_last_name",DBT_INTERNAL_SOURCE."is_admin_user",DBT_INTERNAL_SOURCE."is_marketing_user",DBT_INTERNAL_SOURCE."is_sales_user",DBT_INTERNAL_SOURCE."ao_user_date_created",DBT_INTERNAL_SOURCE."ao_user_account_id",DBT_INTERNAL_SOURCE."ao_user_contact_id",DBT_INTERNAL_SOURCE."deleted",DBT_INTERNAL_SOURCE."deleted_date",DBT_INTERNAL_SOURCE."ao_user_account_name",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "sfdc_ao_instance_user_snapshot2__dbt_tmp160523316939" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;

  