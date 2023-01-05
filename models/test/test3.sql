{{ config(materialized='table') }}

SELECT *
FROM funnel_report_all_time_filters
WHERE date >= '2022-01-01'
AND date <= '2022-12-31'