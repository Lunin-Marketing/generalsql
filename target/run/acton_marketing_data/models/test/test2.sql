

  create  table "acton"."dbt_actonmarketing"."test2__dbt_tmp"
  as (
    

WITH base AS (
    SELECT DISTINCT
        person_source_xf.person_id,
        channel_lead_creation,
        medium_lead_creation,
        source_lead_creation,
        offer_asset_name_lead_creation,
        offer_asset_topic_lead_creation,
        channel_bucket,
        task_subject,
        task_type,
        task_created_date
    FROM "acton"."dbt_actonmarketing"."person_source_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."task_source_xf" ON
    person_source_xf.person_id=task_source_xf.person_id
    WHERE mql_most_recent_date >= '2022-10-01'
    AND channel_bucket IN ('Other','Unknown')
    AND channel_lead_creation IS null
    AND medium_lead_creation IS null
    AND source_lead_creation IS null
    AND offer_asset_name_lead_creation IS null
    AND offer_asset_topic_lead_creation IS null
    

)

SELECT 
    person_id AS leads
FROM base
  );