

  create  table "acton"."dbt_actonmarketing"."funnel_report_all_time_leads_to_target__dbt_tmp"
  as (
    

WITH lead_target AS (

    SELECT 
        kpi_month,
        kpi_region,
        kpi_target AS leads_target
    FROM "acton"."dbt_actonmarketing"."kpi_targets"
    WHERE kpi = 'target_leads'

), actual_leads AS (

    SELECT
        DATE_TRUNC('month',created_date)::Date AS leads_month,
        CASE 
            WHEN global_region LIKE 'NA-%' THEN 'NA'
            WHEN global_region IN ('EUROPE') THEN 'EMEA'
            ELSE null
        END AS region,
        COUNT(DISTINCT lead_id) AS leads
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_leads"
    WHERE created_date >= '2022-04-01'
    GROUP BY 1,2
)

SELECT
    leads_month,
    actual_leads.region,
    leads,
    leads_target,
    leads/leads_target AS actual_vs_target
FROM actual_leads
LEFT JOIN lead_target ON
actual_leads.leads_month=lead_target.kpi_month
AND actual_leads.region=lead_target.kpi_region
WHERE region IS NOT null
  );