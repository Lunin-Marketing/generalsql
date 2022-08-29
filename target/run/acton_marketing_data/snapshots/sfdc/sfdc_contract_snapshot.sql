
      update "acton"."snapshots"."sfdc_contract_snapshot2"
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "sfdc_contract_snapshot2__dbt_tmp160524591941" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = "acton"."snapshots"."sfdc_contract_snapshot2".dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      and "acton"."snapshots"."sfdc_contract_snapshot2".dbt_valid_to is null;

    insert into "acton"."snapshots"."sfdc_contract_snapshot2" ("contract_id", "account_id", "owner_id", "status", "is_deleted", "created_date", "created_by_id", "last_modified_date", "systemmodstamp", "contract_status", "contract_opportunity_id", "churn_date", "cs_churn", "arr", "contract_acv", "subscription_term", "cs_churn_recognized_mrr", "sales_churn", "current_quarter_debook_value", "arr_loss_amount", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."contract_id",DBT_INTERNAL_SOURCE."account_id",DBT_INTERNAL_SOURCE."owner_id",DBT_INTERNAL_SOURCE."status",DBT_INTERNAL_SOURCE."is_deleted",DBT_INTERNAL_SOURCE."created_date",DBT_INTERNAL_SOURCE."created_by_id",DBT_INTERNAL_SOURCE."last_modified_date",DBT_INTERNAL_SOURCE."systemmodstamp",DBT_INTERNAL_SOURCE."contract_status",DBT_INTERNAL_SOURCE."contract_opportunity_id",DBT_INTERNAL_SOURCE."churn_date",DBT_INTERNAL_SOURCE."cs_churn",DBT_INTERNAL_SOURCE."arr",DBT_INTERNAL_SOURCE."contract_acv",DBT_INTERNAL_SOURCE."subscription_term",DBT_INTERNAL_SOURCE."cs_churn_recognized_mrr",DBT_INTERNAL_SOURCE."sales_churn",DBT_INTERNAL_SOURCE."current_quarter_debook_value",DBT_INTERNAL_SOURCE."arr_loss_amount",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "sfdc_contract_snapshot2__dbt_tmp160524591941" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;

  