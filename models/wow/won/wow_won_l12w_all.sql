{{ config(materialized='table') }}

WITH last_12_weeks AS (
SELECT DISTINCT
week 
FROM {{ref('date_base_xf')}}
--FROM "acton".dbt_actonmarketing.date_base_xf
WHERE day BETWEEN CURRENT_DATE-84 AND CURRENT_DATE-7

), final AS (
    
SELECT
date_base_xf.week,
opp_channel_lead_creation,
opp_lead_source,
COUNT(opportunity_id) AS won,
SUM(acv_deal_size_usd)::integer AS acv
FROM {{ref('opp_source_xf')}}
--FROM "acton".dbt_actonmarketing.opp_source_xf
LEFT JOIN {{ref('account_source_xf')}} ON
--LEFT JOIN "acton".dbt_actonmarketing.account_source_xf ON
opp_source_xf.account_id=account_source_xf.account_id
LEFT JOIN {{ref('date_base_xf')}} ON
--LEFT JOIN "acton".dbt_actonmarketing.date_base_xf ON
opp_source_xf.close_date=date_base_xf.day
LEFT JOIN last_12_weeks ON 
date_base_xf.week=last_12_weeks.week
WHERE last_12_weeks.week IS NOT null
AND close_date IS NOT null
AND type = 'New Business'
AND is_won = 'true'
GROUP BY 1,2,3

)
SELECT
week,
opp_channel_lead_creation,
opp_lead_source,
SUM(won) AS won,
SUM(acv)::money AS acv
FROM final
GROUP BY 1,2,3