{{ config(materialized='table') }}

SELECT
    segment,
    opp_lead_source AS lead_source,
    account_global_region AS global_region,
    'DEMO' AS type,
    month,
    quarter,
    COUNT(*) AS records
FROM {{ref('funnel_report_all_time_demo')}}
LEFT JOIN {{ref('date_base_xf')}} ON
funnel_report_all_time_demo.demo_date=date_base_xf.day
WHERE demo_date >= '2021-01-01'
AND opp_lead_source IN ('Marketing','Sales','SDR','Channel')
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2,3,4,5,6