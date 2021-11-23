{{ config(materialized='table') }}

WITH previous_week AS (
SELECT
week
FROM {{ref('date_base_xf')}}
--FROM "acton".dbt_actonmarketing.date_base_xf
WHERE day=CURRENT_DATE-14

), base AS (

SELECT DISTINCT
opp_sales_source_xf.opportunity_id AS won_id,
acv,
opp_sales_source_xf.close_date AS won_date,
country
FROM {{ref('opp_sales_source_xf')}}
--FROM "acton".dbt_actonmarketing.opp_sales_source_xf 
LEFT JOIN {{ref('date_base_xf')}} ON
--LEFT JOIN "acton".dbt_actonmarketing.date_base_xf ON
opp_sales_source_xf.close_date=date_base_xf.day
LEFT JOIN previous_week ON 
date_base_xf.week=previous_week.week
WHERE previous_week.week IS NOT null
AND type = 'New Business'

)

SELECT *
FROM base