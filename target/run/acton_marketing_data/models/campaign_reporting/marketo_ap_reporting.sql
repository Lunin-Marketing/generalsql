

  create  table "acton"."dbt_actonmarketing"."marketo_ap_reporting__dbt_tmp"
  as (
    

SELECT *
FROM "acton"."dbt_actonmarketing"."new_business_acv_influenced_by_aps_FY21_xf"
WHERE LOWER(automated_program_name) LIKE 'marketo'
  );