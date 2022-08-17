

  create  table "acton"."dbt_actonmarketing"."funnel_report_all_time_leads_target__dbt_tmp"
  as (
    

    SELECT 
        kpi_month,
        kpi_region,
        kpi_target AS leads_target
    FROM "acton"."dbt_actonmarketing"."kpi_targets"
    WHERE kpi = 'target_leads'
  );