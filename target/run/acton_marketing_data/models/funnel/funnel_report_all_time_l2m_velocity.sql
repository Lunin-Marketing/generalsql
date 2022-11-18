

  create  table "acton"."dbt_actonmarketing"."funnel_report_all_time_l2m_velocity__dbt_tmp"
  as (
    

WITH leads AS (

    SELECT
        lead_id,
        created_date,
        global_region,
        company_size_rev,
        lead_source,
        is_hand_raiser,
        is_current_customer,
        segment,
        industry,
        channel_bucket,
        channel_bucket_details,
        target_account,
        industry_bucket
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_leads"

),  mqls AS (

    SELECT
        mql_id,
        mql_date,
        global_region,
        company_size_rev,
        lead_source,
        is_hand_raiser,
        is_current_customer,
        segment,
        industry,
        channel_bucket,
        channel_bucket_details,
        target_account,
        industry_bucket
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_mqls"
    
), final AS (

    SELECT
        mql_id,
        mql_date,
        created_date,
        mqls.global_region,
        mqls.company_size_rev,
        mqls.lead_source,
        mqls.segment,
        mqls.is_hand_raiser,
        mqls.is_current_customer,
        mqls.industry,
        mqls.industry_bucket,
        mqls.target_account,
        mqls.channel_bucket,
        mqls.channel_bucket_details,
        
        ((mql_date)::date - (created_date)::date)
     AS l2m_velocity
    FROM mqls
    LEFT JOIN leads ON 
    mqls.mql_id=leads.lead_id
)

SELECT
    global_region,
    company_size_rev,
    lead_source,
    is_hand_raiser,
    is_current_customer,
    segment,
    industry,
    channel_bucket,
    channel_bucket_details,
    industry_bucket,
    target_account,
    mql_date,
    l2m_velocity
FROM final
  );