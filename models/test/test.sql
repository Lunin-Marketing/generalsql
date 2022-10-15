{{ config(materialized='table') }}

SELECT DISTINCT
   person_id
FROM {{ref('funnel_report_all_time_cohort')}}
WHERE mql_created_date >= '2022-08-01'
AND mql_created_date <= '2022-09-30'