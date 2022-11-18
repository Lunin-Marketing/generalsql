

  create  table "acton"."dbt_actonmarketing"."funnel_report_all_time_demo__dbt_tmp"
  as (
    

WITH base AS (

    SELECT DISTINCT
        opp_demo_source_xf.opportunity_id AS demo_id,
        CONCAT('https://acton.my.salesforce.com/',opp_demo_source_xf.opportunity_id) AS demo_url,
        acv,
        account_name,
        type AS opp_type,
        opp_demo_source_xf.demo_date AS demo_date,
        is_current_customer,
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
    END AS channel_bucket,
    CASE
        WHEN channel_bucket_details IS null THEN 'blank'
        ELSE channel_bucket_details
    END AS channel_bucket_details,
    CASE
        WHEN target_account IS null THEN false
        ELSE target_account
    END AS target_account,
    CASE
        WHEN opp_offer_asset_name_lead_creation IS null THEN 'blank'
        ELSE opp_offer_asset_name_lead_creation
    END AS offer_asset_name_lead_creation
    FROM "acton"."dbt_actonmarketing"."opp_demo_source_xf"

)

SELECT *
FROM base
  );