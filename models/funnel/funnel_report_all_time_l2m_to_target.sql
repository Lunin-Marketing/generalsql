{{ config(materialized='table') }}

WITH l2m_target AS (

    SELECT 
        kpi_month,
        kpi_region,
        kpi_target AS l2m_target
    FROM {{ref('kpi_targets')}}
    WHERE kpi = 'target_l2m_conv'

), actual_leads AS (

    SELECT
        DATE_TRUNC('month',created_date)::Date AS leads_month,
        CASE 
            WHEN global_region LIKE 'NA-%' THEN 'NA'
            WHEN global_region IN ('EUROPE') THEN 'EMEA'
            ELSE null
        END AS region,
        COUNT(DISTINCT lead_id) AS leads
    FROM {{ref('funnel_report_all_time_leads')}}
    WHERE created_date >= '2022-04-01'
    GROUP BY 1,2

), actual_mqls AS (

    SELECT
        DATE_TRUNC('month',mql_date)::Date AS mqls_month,
        CASE 
            WHEN global_region LIKE 'NA-%' THEN 'NA'
            WHEN global_region IN ('EUROPE') THEN 'EMEA'
            ELSE null
        END AS region,
        COUNT(DISTINCT mql_id) AS mqls
    FROM {{ref('funnel_report_all_time_mqls')}}
    WHERE mql_date >= '2022-04-01'
    GROUP BY 1,2

), actual_l2m AS (

    SELECT
        mqls_month,
        actual_mqls.region,
        SUM(mqls)/SUM(leads) AS l2m_actual
    FROM actual_mqls
    LEFT JOIN actual_leads ON 
    actual_mqls.region=actual_leads.region
    AND actual_mqls.mqls_month=actual_leads.leads_month
    WHERE actual_mqls.region IS NOT null
    GROUP BY 1,2

)

SELECT
    mqls_month,
    actual_l2m.region,
    l2m_actual,
    l2m_target,
    l2m_actual/l2m_target AS actual_vs_target
FROM actual_l2m
LEFT JOIN l2m_target ON
actual_l2m.mqls_month=l2m_target.kpi_month
AND actual_l2m.region=l2m_target.kpi_region
WHERE region IS NOT null