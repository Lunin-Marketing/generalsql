

  create  table "acton"."dbt_actonmarketing"."test3__dbt_tmp"
  as (
    

WITH user_base AS (

    SELECT
        user_id,
        user_name,
        profile_name
    FROM "acton"."dbt_actonmarketing"."user_source_xf"

)

SELECT
    opportunity_id,
    opp_lead_source,
    profile_name
FROM "acton"."dbt_actonmarketing"."opp_source_xf"
LEFT JOIN user_base ON
opp_source_xf.created_by_id=user_base.user_id
WHERE opp_lead_source IN ('Gleanster Leads','No Show','SFDC-DM|Act On Marketing Automation, Ema','SFDC-dup-DM|Act-On Marketing Automation,','SFDC-dup-IN|ActOn Marketing Automation,','SFDC-IN|Act-On Marketing Automation, Ema','SFDC-IN|ActOn Marketing Automation, Emai','TBD','ZoomInfo','zoominfo may 2011')
OR opp_lead_source LIKE '%LinkedIn / personal contact%'
  );