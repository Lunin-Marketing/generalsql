
      

  create  table "acton"."snapshots"."sfdc_contact_snapshot3"
  as (
    

    select *,
        md5(coalesce(cast(contact_id as varchar ), '')
         || '|' || coalesce(cast(systemmodstamp as varchar ), '')
        ) as dbt_scd_id,
        systemmodstamp as dbt_updated_at,
        systemmodstamp as dbt_valid_from,
        nullif(systemmodstamp, systemmodstamp) as dbt_valid_to
    from (
        



SELECT *
FROM "acton"."dbt_actonmarketing"."contact_source_xf"

    ) sbq



  );
  