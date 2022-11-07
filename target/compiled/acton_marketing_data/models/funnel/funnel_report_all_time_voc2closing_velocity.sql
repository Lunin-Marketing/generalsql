

WITH voc_opp AS (

    SELECT
        voc_id,
        voc_date,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        target_account,
        segment,
        industry,
        channel_bucket,
        industry_bucket
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_voc"

),  closing_opp AS (

    SELECT
        closing_id,
        closing_date,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        target_account,
        segment,
        industry,
        channel_bucket,
        industry_bucket
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_closing"
    
), final AS (

    SELECT
        closing_id,
        voc_date,
        closing_date,
        closing_opp.account_global_region,
        closing_opp.company_size_rev,
        closing_opp.opp_lead_source,
        closing_opp.segment,
        closing_opp.industry,
        closing_opp.target_account,
        closing_opp.industry_bucket,
        closing_opp.channel_bucket,
        
        ((closing_date)::date - (voc_date)::date)
     AS voc2closing_velocity
    FROM closing_opp
    LEFT JOIN voc_opp ON 
    closing_opp.closing_id=voc_opp.voc_id
)

SELECT
    account_global_region,
    company_size_rev,
    opp_lead_source,
    segment,
    industry,
    channel_bucket,
    target_account,
    industry_bucket,
    closing_date,
    voc2closing_velocity
FROM final