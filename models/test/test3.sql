{{ config(materialized='table') }}

SELECT
    sqo_id,
    acv
FROM {{ref('funnel_report_all_time_pipeline')}}
WHERE created_date >= '2022-10-01'
AND created_date <= '2022-12-31'
AND sqo_date >= '2022-10-01'
AND sqo_date <= '2022-12-31'
AND opp_type = 'New Business'