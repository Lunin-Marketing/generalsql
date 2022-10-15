{{ config(materialized='table') }}

WITH  base AS (

SELECT
opportunity_id,
opportunity_name,
owner_id,
discovery_call_scheduled_datetime
FROM {{ref('opp_source_xf')}}
--FROM "acton".dbt_actonmarketing.opp_source_xf
WHERE 1=1
AND type = 'New Business'
AND discovery_call_scheduled_datetime IS NOT null
AND discovery_date IS null
AND opportunity_status != 'Rescheduling Meeting'
AND is_closed != 'true'
AND is_won != 'true'
AND is_deleted != 'true'
)

SELECT
discovery_call_scheduled_datetime,
COUNT(DISTINCT opportunity_id) AS opps
FROM base
GROUP BY 1