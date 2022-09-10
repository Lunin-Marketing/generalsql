

  create  table "acton"."dbt_actonmarketing"."test__dbt_tmp"
  as (
    

SELECT
    person_id,
    lead_source,
    created_by_name
FROM "acton"."dbt_actonmarketing"."person_source_xf"
WHERE (lead_source NOT IN ('Marketing','Sales','Channel','SDR','Zendesk')
    OR lead_source IS null)
AND created_date >= '2021-01-01'
  );