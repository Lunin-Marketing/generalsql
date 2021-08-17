{{ config(materialized='table') }}

WITH current_week AS (
SELECT
week
FROM "acton".dbt_actonmarketing.date_base_xf
WHERE day=CURRENT_DATE-7
), base AS (
SELECT DISTINCT
opp_sales_source_xf.opportunity_id AS won_id,
opp_sales_source_xf.close_date AS won_date
FROM "acton".dbt_actonmarketing.opp_sales_source_xf 
LEFT JOIN "acton".dbt_actonmarketing.date_base_xf ON
opp_sales_source_xf.close_date=date_base_xf.day
LEFT JOIN current_week ON 
date_base_xf.week=current_week.week
WHERE current_week.week IS NOT null
)

SELECT *
FROM base