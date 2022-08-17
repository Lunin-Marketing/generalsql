

    SELECT 
        kpi_month,
        kpi_region,
        kpi_target AS leads_target
    FROM "acton"."dbt_actonmarketing"."kpi_targets"
    WHERE kpi = 'target_leads'