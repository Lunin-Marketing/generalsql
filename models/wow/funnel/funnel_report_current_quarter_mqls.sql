{{ config(materialized='table') }}

WITH current_quarter AS (
SELECT
fy,
quarter 
FROM "acton".dbt_actonmarketing.date_base_xf
WHERE day=CURRENT_DATE
), base AS (
SELECT DISTINCT
lead_mql_source_xf.lead_id AS mql_id,
lead_mql_source_xf.mql_created_date AS mql_date
FROM "acton".dbt_actonmarketing.lead_mql_source_xf
LEFT JOIN "acton".dbt_actonmarketing.date_base_xf ON
lead_mql_source_xf.mql_created_date=date_base_xf.day
LEFT JOIN current_quarter ON 
date_base_xf.quarter=current_quarter.quarter
WHERE current_quarter.quarter IS NOT null
)

SELECT *
FROM base