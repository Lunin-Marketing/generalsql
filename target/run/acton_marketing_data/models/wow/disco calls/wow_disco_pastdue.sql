

  create  table "acton"."dbt_actonmarketing"."wow_disco_pastdue__dbt_tmp"
  as (
    

WITH  base AS (

SELECT
opportunity_id,
opportunity_name,
user_name AS owner_name,
discovery_call_scheduled_datetime
FROM "acton"."dbt_actonmarketing"."opp_source_xf"
--FROM "acton".dbt_actonmarketing.opp_source_xf
LEFT JOIN "acton"."dbt_actonmarketing"."user_source_xf" ON
--LEFT JOIN "acton".dbt_actonmarketing.user_source_xf ON
opp_source_xf.owner_id=user_source_xf.user_id
WHERE 1=1
AND type = 'New Business'
AND discovery_call_scheduled_datetime IS NOT null
AND discovery_date IS null
AND opportunity_status != 'Rescheduling Meeting'
AND is_closed != 'true'
AND is_won != 'true'
AND is_deleted != 'true'
AND discovery_call_scheduled_datetime < CURRENT_DATE
)

SELECT
owner_name,
COUNT(DISTINCT opportunity_id) AS opps
FROM base
GROUP BY 1
  );