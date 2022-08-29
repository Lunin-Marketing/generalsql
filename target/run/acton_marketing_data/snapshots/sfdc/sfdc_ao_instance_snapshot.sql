
      update "acton"."snapshots"."sfdc_ao_instance_snapshot2"
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "sfdc_ao_instance_snapshot2__dbt_tmp160523339880" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = "acton"."snapshots"."sfdc_ao_instance_snapshot2".dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      and "acton"."snapshots"."sfdc_ao_instance_snapshot2".dbt_valid_to is null;

    insert into "acton"."snapshots"."sfdc_ao_instance_snapshot2" ("ao_instance_id", "is_deleted", "ao_instance_name", "ao_instance_created_date", "last_modified_date", "systemmodstamp", "ao_account_name", "cdb_ao_instance_id", "ao_instance_account", "ao_instance_parent_account", "ao_instance_parent_account_id", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."ao_instance_id",DBT_INTERNAL_SOURCE."is_deleted",DBT_INTERNAL_SOURCE."ao_instance_name",DBT_INTERNAL_SOURCE."ao_instance_created_date",DBT_INTERNAL_SOURCE."last_modified_date",DBT_INTERNAL_SOURCE."systemmodstamp",DBT_INTERNAL_SOURCE."ao_account_name",DBT_INTERNAL_SOURCE."cdb_ao_instance_id",DBT_INTERNAL_SOURCE."ao_instance_account",DBT_INTERNAL_SOURCE."ao_instance_parent_account",DBT_INTERNAL_SOURCE."ao_instance_parent_account_id",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "sfdc_ao_instance_snapshot2__dbt_tmp160523339880" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;

  