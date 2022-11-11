

  create  table "acton"."dbt_actonmarketing"."test2__dbt_tmp"
  as (
    

SELECT
    person_id
FROM "acton"."dbt_actonmarketing"."person_source_xf"
WHERE channel_lead_creation IS null
AND medium_lead_creation  IS null
AND source_lead_creation IS null
AND created_date < '2020-06-01'
  );