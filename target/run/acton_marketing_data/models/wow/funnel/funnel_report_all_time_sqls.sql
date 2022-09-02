

  create  table "acton"."dbt_actonmarketing"."funnel_report_all_time_sqls__dbt_tmp"
  as (
    

WITH base AS (

    SELECT DISTINCT
        sql_source_xf.sql_id,
        CONCAT('https://acton.my.salesforce.com/',sql_source_xf.sql_id) AS sql_url,
        sql_source_xf.created_date AS sql_date,
        account_name,
        opportunity_name,
        type,
        owner_name,
        sdr_name,
        CASE
        WHEN account_global_region IS null THEN 'blank'
        ELSE account_global_region
    END AS account_global_region,
    CASE
        WHEN company_size_rev IS null THEN 'blank'
        ELSE company_size_rev
    END AS company_size_rev,
    CASE
        WHEN opp_lead_source IS null THEN 'blank'
        ELSE opp_lead_source
    END AS opp_lead_source,
    CASE
        WHEN segment IS null THEN 'blank'
        ELSE segment
    END AS segment,
    CASE
        WHEN industry IS null THEN 'blank'
        ELSE industry
    END AS industry,
    CASE
        WHEN industry_bucket IS null THEN 'blank'
        ELSE industry_bucket
    END AS industry_bucket,
    CASE
        WHEN channel_bucket IS null THEN 'blank'
        ELSE channel_bucket
    END AS channel_bucket
    FROM "acton"."dbt_actonmarketing"."sql_source_xf"
    WHERE type = 'New Business'

)

SELECT *
FROM base
  );