{{ config(materialized='table') }}

WITH last_12_weeks AS (
SELECT DISTINCT
week 
FROM "acton".dbt_actonmarketing.date_base_xf
WHERE day BETWEEN CURRENT_DATE-84 AND CURRENT_DATE-7

), opp_final AS (
SELECT
created_date,
opportunity_id
FROM "acton".dbt_actonmarketing.opp_source_xf
LEFT JOIN "acton".dbt_actonmarketing.account_source_xf ON
opp_source_xf.account_id=account_source_xf.account_id
WHERE 1=1 
AND created_date IS NOT null
AND stage_name NOT IN ('Closed - Duplicate','Closed - Admin Removed')
AND company_size_rev = 'Mid-Market' 
AND type = 'New Business'
), final AS (
    
SELECT
last_12_weeks.week,
COUNT(opportunity_id) AS sqls
FROM last_12_weeks
LEFT JOIN "acton".dbt_actonmarketing.date_base_xf ON
date_base_xf.week=last_12_weeks.week
LEFT JOIN opp_final ON 
opp_final.created_date=date_base_xf.day
WHERE 1=1 
AND last_12_weeks.week IS NOT null
GROUP BY 1

)
SELECT
week,
SUM(sqls) AS sqls
FROM final
GROUP BY 1