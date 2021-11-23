{{ config(materialized='table') }}

WITH base_pipeline AS (

SELECT
opportunity_id,
opp_source_lead_creation,
acv_deal_size_usd
FROM {{ref('opp_source_xf')}}
--FROM "acton".dbt_actonmarketing.opp_source_xf
WHERE discovery_date >= '2020-01-01'
AND discovery_date <= '2021-06-21'
AND type = 'New Business'
AND stage_name != 'Closed - Duplicate'
AND stage_name != 'Closed - Admin Removed'
AND opp_channel_lead_creation = 'ppc'
AND opp_source_lead_creation NOT LIKE ('%act-on%')
AND opp_source_lead_creation NOT LIKE ('%linkedin%')
AND opp_source_lead_creation NOT LIKE ('%social%')
AND opp_source_lead_creation NOT LIKE ('%SoftwareAdvice%')

)

SELECT
opp_source_lead_creation,
COUNT(DISTINCT opportunity_id),
SUM(acv_deal_size_usd)
FROM base_pipeline
GROUP BY 1