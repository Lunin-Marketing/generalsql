

  create  table "acton"."dbt_actonmarketing"."funnel_report_all_time_sqo2won_velocity__dbt_tmp"
  as (
    

WITH sqo_opp AS (

    SELECT
        sqo_id,
        sqo_date,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        channel_bucket,
        industry_bucket
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_sqos"

), won_opp AS (

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
    
), final AS (

    SELECT
        won_id,
        won_date,
        sqo_date,
        won_opp.account_global_region,
        won_opp.company_size_rev,
        won_opp.opp_lead_source,
        won_opp.segment,
        won_opp.industry,
        won_opp.channel_bucket,
        won_opp.industry_bucket,
        

    
        ((won_date)::date - (sqo_date)::date)
    

 AS sqo2won_velocity
    FROM won_opp
    LEFT JOIN sqo_opp ON 
    won_opp.won_id=sqo_opp.sqo_id
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
    sqo2won_velocity
FROM final
  );