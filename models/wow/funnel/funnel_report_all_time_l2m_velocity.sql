{{ config(materialized='table') }}

WITH leads AS (

    SELECT
        lead_id,
        created_date,
        global_region,
        company_size_rev,
        lead_source,
        segment,
        industry,
        channel_bucket
    FROM {{ref('funnel_report_all_time_leads')}}

),  mqls AS (

    SELECT
        mql_id,
        mql_date,
        global_region,
        company_size_rev,
        lead_source,
        segment,
        industry,
        channel_bucket
    FROM {{ref('funnel_report_all_time_mqls')}}
    
), final AS (

    SELECT
        mql_id,
        mql_date,
        created_date,
        mqls.global_region,
        mqls.company_size_rev,
        mqls.lead_source,
        mqls.segment,
        mqls.industry,
        mqls.channel_bucket,
        {{ dbt_utils.datediff("created_date","mql_date",'day')}} AS l2m_velocity
    FROM mqls
    LEFT JOIN leads ON 
    mqls.mql_id=leads.lead_id
)

SELECT
    global_region,
    company_size_rev,
    lead_source,
    segment,
    industry,
    channel_bucket,
    mql_date,
    l2m_velocity
FROM final