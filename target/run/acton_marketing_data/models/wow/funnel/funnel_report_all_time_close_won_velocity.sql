

  create  table "acton"."dbt_actonmarketing"."funnel_report_all_time_close_won_velocity__dbt_tmp"
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
        industry_bucket,
        channel_bucket
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_won"

),  sql_opp AS (

    SELECT
        sql_id,
        sql_date,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        industry_bucket,
        channel_bucket
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_sqls"
    
), final AS (

    SELECT
        won_id,
        sql_date,
        won_date,
        won_opp.account_global_region,
        won_opp.company_size_rev,
        won_opp.opp_lead_source,
        won_opp.segment,
        won_opp.industry,
        won_opp.industry_bucket,
        won_opp.channel_bucket,
        

    
        ((won_date)::date - (sql_date)::date)
    

 AS cw_velocity
    FROM won_opp
    LEFT JOIN sql_opp ON 
    won_opp.won_id=sql_opp.sql_id
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
    cw_velocity
FROM final
  );