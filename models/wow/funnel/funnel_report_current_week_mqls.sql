{{ config(materialized='table') }}

WITH current_week AS (
SELECT
week
FROM "acton".dbt_actonmarketing.date_base_xf
WHERE day=CURRENT_DATE-7
), base AS (
SELECT DISTINCT
lead_mql_source_xf.lead_id AS mql_id,
lead_mql_source_xf.mql_created_date AS mql_date,
country
FROM "acton".dbt_actonmarketing.lead_mql_source_xf
LEFT JOIN "acton".dbt_actonmarketing.date_base_xf ON
lead_mql_source_xf.mql_created_date=date_base_xf.day
LEFT JOIN current_week ON 
date_base_xf.week=current_week.week
WHERE current_week.week IS NOT null
)

SELECT 
*
FROM base