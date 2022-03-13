{{ config(materialized='table') }}

WITH previous_fy AS (
SELECT
fy
FROM {{ref('date_base_xf')}}
WHERE fy='2021'
), base AS (
SELECT DISTINCT
lead_mql_source_xf.person_id AS mql_id,
lead_mql_source_xf.mql_created_date AS mql_date
FROM {{ref('lead_mql_source_xf')}}
LEFT JOIN {{ref('date_base_xf')}} ON
lead_mql_source_xf.mql_created_date=date_base_xf.day
LEFT JOIN previous_fy ON 
date_base_xf.fy=previous_fy.fy
WHERE previous_fy.fy IS NOT null
)

SELECT *
FROM base