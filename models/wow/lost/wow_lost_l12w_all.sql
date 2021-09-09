{{ config(materialized='table') }}

WITH last_12_weeks AS (
SELECT DISTINCT
week 
FROM "acton".dbt_actonmarketing.date_base_xf
WHERE day BETWEEN CURRENT_DATE-84 AND CURRENT_DATE-7

), final AS (
    
SELECT
date_base_xf.week,
opp_channel_lead_creation,
opp_lead_source,
COUNT(opportunity_id) AS won,
SUM(acv_deal_size_usd) AS acv
FROM "acton".dbt_actonmarketing.opp_source_xf
LEFT JOIN "acton".dbt_actonmarketing.account_source_xf ON
opp_source_xf.account_id=account_source_xf.account_id
LEFT JOIN "acton".dbt_actonmarketing.date_base_xf ON
opp_source_xf.close_date=date_base_xf.day
LEFT JOIN last_12_weeks ON 
date_base_xf.week=last_12_weeks.week
WHERE last_12_weeks.week IS NOT null
AND close_date IS NOT null
AND discovery_date IS NOT null
AND type = 'New Business'
AND stage_name IN ('Closed – Lost No Resources / Budget','Closed – Lost Not Ready / No Decision','Closed – Lost Product Deficiency','Closed - Lost to Competitor')
GROUP BY 1,2,3

)
SELECT
week,
opp_channel_lead_creation,
opp_lead_source,
SUM(won) AS won,
SUM(acv) AS acv
FROM final
GROUP BY 1,2,3