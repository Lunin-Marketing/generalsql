

  create  table "acton"."dbt_actonmarketing"."test4__dbt_tmp"
  as (
    

SELECT DISTINCT
    opportunity_id,
    opp_lead_source
FROM "acton"."dbt_actonmarketing"."opp_source_xf"
WHERE opp_lead_source NOT IN ('Marketing','SDR','Sales','Channel','CSM','Zendesk')
  );