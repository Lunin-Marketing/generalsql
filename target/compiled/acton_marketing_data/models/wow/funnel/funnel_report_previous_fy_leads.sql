

WITH previous_fy AS (
SELECT
fy
FROM "acton"."dbt_actonmarketing"."date_base_xf"
WHERE fy='2021'
), base AS (
SELECT DISTINCT
lead_source_xf.lead_id AS lead_id,
lead_source_xf.marketing_created_date AS created_date
FROM "acton"."dbt_actonmarketing"."lead_source_xf"
LEFT JOIN "acton"."dbt_actonmarketing"."date_base_xf" ON
lead_source_xf.marketing_created_date=date_base_xf.day
LEFT JOIN previous_fy ON 
date_base_xf.fy=previous_fy.fy
WHERE previous_fy.fy IS NOT null
)

SELECT *
FROM base