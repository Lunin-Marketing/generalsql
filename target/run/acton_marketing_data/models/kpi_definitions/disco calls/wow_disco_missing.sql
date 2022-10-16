

  create  table "acton"."dbt_actonmarketing"."wow_disco_missing__dbt_tmp"
  as (
    

WITH base AS (

SELECT
opportunity_id,
user_name AS owner_name
FROM "acton"."dbt_actonmarketing"."opp_source_xf"
--FROM "acton".dbt_actonmarketing.opp_source_xf
LEFT JOIN "acton"."dbt_actonmarketing"."user_source_xf" ON
--LEFT JOIN "acton".dbt_actonmarketing.user_source_xf ON
opp_source_xf.created_by_id=user_source_xf.user_id
WHERE 1=1
AND type = 'New Business'
AND discovery_call_scheduled_datetime IS null
AND discovery_date IS null
AND is_closed != 'true'
AND is_won != 'true'
AND is_deleted != 'true'

)

SELECT
owner_name,
COUNT(DISTINCT opportunity_id) AS opps
FROM base
GROUP BY 1
  );