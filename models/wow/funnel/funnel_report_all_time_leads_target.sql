{{ config(materialized='table') }}

    SELECT 
        kpi_month,
        kpi_region,
        kpi_target AS leads_target
    FROM {{ref('kpi_targets')}}
    WHERE kpi = 'target_leads'