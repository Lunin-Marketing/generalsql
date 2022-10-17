

  create  table "acton"."dbt_actonmarketing"."funnel_report_all_time_clos2cw_velocity__dbt_tmp"
  as (
    

WITH won_opp AS (

    SELECT
        won_id,
        won_date,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        industry_bucket
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_won"

),  closing_opp AS (

    SELECT
        closing_id,
        closing_date,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        industry_bucket,
        channel_bucket
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_closing"
    
), final AS (

    SELECT
        won_id,
        closing_date,
        won_date,
        won_opp.account_global_region,
        won_opp.company_size_rev,
        won_opp.opp_lead_source,
        won_opp.segment,
        won_opp.industry,
        won_opp.industry_bucket,
        won_opp.channel_bucket,
        
        ((won_date)::date - (closing_date)::date)
     AS closing2cw_velocity
    FROM won_opp
    LEFT JOIN closing_opp ON 
    won_opp.won_id=closing_opp.closing_id
)

SELECT
    account_global_region,
    company_size_rev,
    opp_lead_source,
    segment,
    industry,
    channel_bucket,
    industry_bucket,
    won_date,
    closing2cw_velocity
FROM final
  );