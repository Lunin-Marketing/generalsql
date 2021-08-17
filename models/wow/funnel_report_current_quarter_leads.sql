{{ config(materialized='table') }}

WITH current_quarter AS (
SELECT
fy,
quarter 
FROM "acton".dbt_actonmarketing.date_base_xf
WHERE day=CURRENT_DATE
), base AS (
SELECT DISTINCT
lead_source_xf.lead_id AS lead_id,
--lead_mql_source_xf.lead_id AS mql_id,
--sal_source_xf.lead_id AS sal_id,
--sql_source_xf.opportunity_id AS sql_id,
--sqo_source_xf.opportunity_id AS sqo_id,
--opp_sales_source_xf.opportunity_id AS won_id,
lead_source_xf.created_date
FROM "acton".dbt_actonmarketing.lead_source_xf
--LEFT JOIN "acton".dbt_actonmarketing.lead_mql_source_xf ON
--lead_source_xf.created_date=lead_mql_source_xf.mql_created_date
--LEFT JOIN "acton".dbt_actonmarketing.sal_source_xf ON
--lead_source_xf.created_date=sal_source_xf.mql_created_date
--LEFT JOIN "acton".dbt_actonmarketing.sql_source_xf ON
--lead_source_xf.created_date=sql_source_xf.created_date
--LEFT JOIN "acton".dbt_actonmarketing.sqo_source_xf ON
--lead_source_xf.created_date=sqo_source_xf.discovery_date
--LEFT JOIN "acton".dbt_actonmarketing.opp_sales_source_xf ON
--lead_source_xf.created_date=opp_sales_source_xf.close_date
LEFT JOIN "acton".dbt_actonmarketing.date_base_xf ON
lead_source_xf.created_date=date_base_xf.day
LEFT JOIN current_quarter ON 
date_base_xf.quarter=current_quarter.quarter
WHERE current_quarter.quarter IS NOT null
)

SELECT *
FROM base