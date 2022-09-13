

  create  table "acton"."dbt_actonmarketing"."topo_closing__dbt_tmp"
  as (
    

SELECT
    segment,
    opp_lead_source AS lead_source,
    account_global_region AS global_region,
    'CLOSING' AS type,
    month,
    quarter,
    COUNT(*) AS records
FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_closing"
LEFT JOIN "acton"."dbt_actonmarketing"."date_base_xf" ON
funnel_report_all_time_closing.closing_date=date_base_xf.day
WHERE closing_date >= '2021-01-01'
AND opp_lead_source IN ('Marketing','Sales','SDR','Channel')
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2,3,4,5,6
  );