{{ config(materialized='table') }}
WITH final AS (
SELECT
lead_id,
email,
lead_source,
is_hand_raiser,
lead_status,
day AS lead_created_day,
week AS lead_created_week,
month AS lead_created_month
FROM "defaultdb".dbt_acton.lead_source_xf
LEFT JOIN "defaultdb".dbt_acton.date_base_xf ON
lead_source_xf.lead_created_date=date_base_xf.day
)
SELECT *
FROM final
WHERE lead_created_day IS NOT null