{{ config(materialized='table') }}

WITH previous_week AS (
SELECT
week 
FROM {{ref('date_base_xf')}}
--FROM "acton".dbt_actonmarketing.date_base_xf
WHERE day=CURRENT_DATE-14

), base AS (

SELECT DISTINCT
lead_source_xf.lead_id AS lead_id,
lead_source_xf.marketing_created_date AS created_date,
country
FROM {{ref('lead_source_xf')}}
--FROM "acton".dbt_actonmarketing.lead_source_xf
LEFT JOIN {{ref('date_base_xf')}} ON
--LEFT JOIN "acton".dbt_actonmarketing.date_base_xf ON
lead_source_xf.marketing_created_date=date_base_xf.day
LEFT JOIN previous_week ON 
date_base_xf.week=previous_week.week
WHERE previous_week.week IS NOT null

)

SELECT *
FROM base