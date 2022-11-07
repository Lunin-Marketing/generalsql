

WITH sals AS (

    SELECT
        sal_id,
        sal_date,
        global_region,
        company_size_rev,
        lead_source,
        segment,
        industry,
        target_account,
        channel_bucket,
        industry_bucket
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_sals"

),  mqls AS (

    SELECT
        mql_id,
        mql_date,
        global_region,
        company_size_rev,
        lead_source,
        segment,
        industry,
        target_account,
        channel_bucket,
        industry_bucket
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_mqls"
    
), final AS (

    SELECT
        sal_id,
        sal_date,
        mql_date,
        sals.global_region,
        sals.company_size_rev,
        sals.lead_source,
        sals.segment,
        sals.industry,
        sals.target_account,
        sals.channel_bucket,
        sals.industry_bucket,
        
        ((sal_date)::date - (mql_date)::date)
     AS m2sal_velocity
    FROM sals
    LEFT JOIN mqls ON 
    sals.sal_id=mqls.mql_id
)

SELECT
    global_region,
    company_size_rev,
    lead_source,
    segment,
    industry,
    channel_bucket,
    target_account,
    industry_bucket,
    sal_date,
    m2sal_velocity
FROM final