{{ config(materialized='table') }}
WITH final AS (
SELECT
lead_id,
email,
lead_source,
is_hand_raiser,
lead_status,
day AS lead_mql_day,
week AS lead_mql_week,
month AS lead_mql_month
FROM "acton".dbt_acton.lead_source_xf
LEFT JOIN "acton".dbt_acton.date_base_xf ON
lead_source_xf.mql_created_date=date_base_xf.day
)
SELECT *
FROM final
WHERE lead_mql_day IS NOT null