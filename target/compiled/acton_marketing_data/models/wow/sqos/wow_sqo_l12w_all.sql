

WITH last_12_weeks AS (
SELECT DISTINCT
week 
FROM "acton"."dbt_actonmarketing"."date_base_xf"
--FROM "acton".dbt_actonmarketing.date_base_xf
WHERE day BETWEEN CURRENT_DATE-84 AND CURRENT_DATE-7

), final AS (
    
SELECT
last_12_weeks.week,
COUNT(DISTINCT opportunity_id) AS sqos,
SUM(acv_deal_size_usd) AS acv
FROM "acton"."dbt_actonmarketing"."opp_source_xf"
--FROM "acton".dbt_actonmarketing.opp_source_xf
LEFT JOIN "acton"."dbt_actonmarketing"."account_source_xf" ON
--LEFT JOIN "acton".dbt_actonmarketing.account_source_xf ON
opp_source_xf.account_id=account_source_xf.account_id
LEFT JOIN "acton"."dbt_actonmarketing"."date_base_xf" ON
--LEFT JOIN "acton".dbt_actonmarketing.date_base_xf ON
opp_source_xf.discovery_date=date_base_xf.day
LEFT JOIN last_12_weeks ON 
date_base_xf.week=last_12_weeks.week
WHERE last_12_weeks.week IS NOT null
AND discovery_date IS NOT null
AND stage_name NOT IN ('Closed - Duplicate','Closed - Admin Removed')
AND type = 'New Business'
GROUP BY 1

)
SELECT
week,
SUM(sqos) AS sqos,
SUM(acv) AS acv
FROM final
GROUP BY 1