{{ config(materialized='table') }}

WITH base AS (

SELECT
opportunity_id,
owner_id AS owner_name --change to created_by
FROM "acton".dbt_actonmarketing.opp_source_xf
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