{{ config(materialized='table') }}

WITH base AS (
SELECT
COUNT(DISTINCT lead_id) AS leads,
0 AS leads_acv,
0 AS mqls,
0 AS mql_acv,
0 AS sals,
0 AS sal_acv,
0 AS sqls,
0 AS sql_acv,
0 AS sqos,
0 AS sqo_acv,
0 AS won,
0 AS won_acv,
0 AS lost,
0 AS lost_acv,
0 AS churn,
0 AS churn_acv
FROM "acton".dbt_actonmarketing.funnel_report_current_week_leads
WHERE country IN ('UK','United Kingdom','GB','IE')
GROUP BY 2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
UNION ALL
SELECT
0 AS leads,
0 AS leads_acv,
COUNT(DISTINCT mql_id) AS mqls,
0 AS mql_acv,
0 AS sals,
0 AS sal_acv,
0 AS sqls,
0 AS sql_acv,
0 AS sqos,
0 AS sqo_acv,
0 AS won,
0 AS won_acv,
0 AS lost,
0 AS lost_acv,
0 AS churn,
0 AS churn_acv
FROM "acton".dbt_actonmarketing.funnel_report_current_week_mqls
WHERE country IN ('UK','United Kingdom','GB','IE')
GROUP BY 1,2,4,5,6,7,8,9,10,11,12,13,14,15,16
UNION ALL
SELECT
0 AS leads,
0 AS leads_acv,
0 AS mqls,
0 AS mql_acv,
COUNT(DISTINCT sal_id) AS sals,
0 AS sal_acv,
0 AS sqls,
0 AS sql_acv,
0 AS sqos,
0 AS sqo_acv,
0 AS won,
0 AS won_acv,
0 AS lost,
0 AS lost_acv,
0 AS churn,
0 AS churn_acv
FROM "acton".dbt_actonmarketing.funnel_report_current_week_sals
WHERE country IN ('UK','United Kingdom','GB','IE')
GROUP BY 1,2,3,4,6,7,8,9,10,11,12,13,14,15,16
UNION ALL
SELECT
0 AS leads,
0 AS leads_acv,
0 AS mqls,
0 AS mql_acv,
0 AS sals,
0 AS sal_acv,
COUNT(DISTINCT sql_id) AS sqls,
0 AS sql_acv,
0 AS sqos,
0 AS sqo_acv,
0 AS won,
0 AS won_acv,
0 AS lost,
0 AS lost_acv,
0 AS churn,
0 AS churn_acv
FROM "acton".dbt_actonmarketing.funnel_report_current_week_sqls
WHERE country IN ('UK','United Kingdom','GB','IE')
GROUP BY 1,2,3,4,5,6,8,9,10,11,12,13,14,15,16
UNION ALL
SELECT
0 AS leads,
0 AS leads_acv,
0 AS mqls,
0 AS mql_acv,
0 AS sals,
0 AS sal_acv,
0 AS sqls,
0 AS sql_acv,
COUNT(DISTINCT sqo_id) AS sqos,
SUM(acv) AS sqo_acv,
0 AS won,
0 AS won_acv,
0 AS lost,
0 AS lost_acv,
0 AS churn,
0 AS churn_acv
FROM "acton".dbt_actonmarketing.funnel_report_current_week_sqos
WHERE country IN ('UK','United Kingdom','GB','IE')
GROUP BY 1,2,4,3,4,5,6,7,8,11,12,13,14,15,16
UNION ALL
SELECT
0 AS leads,
0 AS leads_acv,
0 AS mqls,
0 AS mql_acv,
0 AS sals,
0 AS sal_acv,
0 AS sqls,
0 AS sql_acv,
0 AS sqos,
0 AS sqo_acv,
COUNT(DISTINCT won_id) AS won,
SUM(acv) AS won_acv,
0 AS lost,
0 AS lost_acv,
0 AS churn,
0 AS churn_acv
FROM "acton".dbt_actonmarketing.funnel_report_current_week_won
WHERE country IN ('UK','United Kingdom','GB','IE')
GROUP BY 1,2,4,3,4,5,6,7,8,9,10,13,14,15,16
UNION ALL
SELECT
0 AS leads,
0 AS leads_acv,
0 AS mqls,
0 AS mql_acv,
0 AS sals,
0 AS sal_acv,
0 AS sqls,
0 AS sql_acv,
0 AS sqos,
0 AS sqo_acv,
0 AS won,
0 AS won_acv,
COUNT(DISTINCT opportunity_id) AS lost,
SUM(acv_deal_size_usd) AS lost_acv,
0 AS churn,
0 AS churn_acv
FROM "acton".dbt_actonmarketing.funnel_report_current_week_lost
WHERE country IN ('UK','United Kingdom','GB','IE')
GROUP BY 1,2,4,3,4,5,6,7,8,9,10,11,12,15,16
UNION ALL
SELECT
0 AS leads,
0 AS leads_acv,
0 AS mqls,
0 AS mql_acv,
0 AS sals,
0 AS sal_acv,
0 AS sqls,
0 AS sql_acv,
0 AS sqos,
0 AS sqo_acv,
0 AS won,
0 AS won_acv,
0 AS lost,
0 AS lost_acv,
COUNT(DISTINCT contract_id) AS churn,
SUM(arr_loss_amount) AS churn_acv
FROM "acton".dbt_actonmarketing.funnel_report_current_week_churn
WHERE country IN ('UK','United Kingdom','GB','IE')
GROUP BY 1,2,4,3,4,5,6,7,8,9,10,11,12,13,14

)
SELECT
SUM(leads) AS leads,
SUM(leads_acv) AS lead_acv,
SUM(mqls) AS mqls,
SUM(mql_acv) AS mql_acv,
SUM(sals) AS sals,
SUM(sal_acv) AS sal_acv,
SUM(sqls) AS sqls,
SUM(sql_acv) AS sql_acv,
SUM(sqos) AS sqos,
SUM(sqo_acv) AS sqo_acv,
SUM(won) AS won,
SUM(won_acv) AS won_acv,
SUM(lost) AS lost,
SUM(lost_acv) AS lost_acv,
SUM(churn) AS churn,
SUM(churn_acv) AS churn_acv
FROM base