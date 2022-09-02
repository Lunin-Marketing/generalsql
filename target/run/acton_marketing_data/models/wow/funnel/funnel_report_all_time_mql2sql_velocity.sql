

  create  table "acton"."dbt_actonmarketing"."funnel_report_all_time_mql2sql_velocity__dbt_tmp"
  as (
    

WITH sqls AS (

    SELECT
        sql_id,
        sql_date,
        account_global_region AS global_region,
        company_size_rev,
        opp_lead_source AS lead_source,
        segment,
        industry,
        channel_bucket,
        industry_bucket
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_sqls"

),  mqls AS (

    SELECT
        mql_id,
        mql_date,
        global_region,
        company_size_rev,
        lead_source,
        segment,
        industry,
        channel_bucket,
        industry_bucket
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_mqls"
    
), final AS (

    SELECT
        sql_id,
        sql_date,
        mql_date,
        sqls.global_region,
        sqls.company_size_rev,
        sqls.lead_source,
        sqls.segment,
        sqls.industry,
        sqls.channel_bucket,
        sqls.industry_bucket,
        

    
        ((sql_date)::date - (mql_date)::date)
    

 AS mql2sql_velocity
    FROM sqls
    LEFT JOIN mqls ON 
    sqls.sql_id=mqls.mql_id
)

SELECT
    global_region,
    company_size_rev,
    lead_source,
    segment,
    industry,
    channel_bucket,
    industry_bucket,
    sql_date,
    mql2sql_velocity
FROM final
  );