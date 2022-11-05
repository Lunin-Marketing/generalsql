

  create  table "acton"."dbt_actonmarketing"."funnel_report_all_time_filters__dbt_tmp"
  as (
    

WITH base AS (

    SELECT DISTINCT
        global_region,
        company_size_rev,
        lead_source,
        segment,
        industry,
        industry_bucket,
        channel_bucket,
        target_account,
        is_hand_raiser,
        offer_asset_name_lead_creation,
        created_date AS date,
        null AS opp_type,
        null AS is_working
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_leads"
    UNION ALL
    SELECT DISTINCT
        global_region,
        company_size_rev,
        lead_source,
        segment,
        industry,
        industry_bucket,
        channel_bucket,
        target_account,
        is_hand_raiser,
        offer_asset_name_lead_creation,
        mql_date,
        null AS opp_type,
        is_working
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_mqls"
    UNION ALL
    SELECT DISTINCT
        global_region,
        company_size_rev,
        lead_source,
        segment,
        industry,
        industry_bucket,
        channel_bucket,
        target_account,
        is_hand_raiser,
        offer_asset_name_lead_creation,
        sal_date,
        null AS opp_type,
        null AS is_working
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_sals"
    UNION ALL
    SELECT DISTINCT
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        industry_bucket,
        channel_bucket,
        target_account,
        'No' AS is_hand_raiser,
        offer_asset_name_lead_creation,
        sql_date,
        opp_type,
        null AS is_working
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_sqls"
    UNION ALL
    SELECT DISTINCT
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        industry_bucket,
        channel_bucket,
        target_account,
        'No' AS is_hand_raiser,
        offer_asset_name_lead_creation,
        sqo_date,
        opp_type,
        null AS is_working
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_sqos"
    UNION ALL
    SELECT DISTINCT
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        industry_bucket,
        channel_bucket,
        target_account,
        'No' AS is_hand_raiser,
        offer_asset_name_lead_creation,
        demo_date,
        opp_type,
        null AS is_working
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_demo"
    UNION ALL
    SELECT DISTINCT
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        industry_bucket,
        channel_bucket,
        target_account,
        'No' AS is_hand_raiser,
        offer_asset_name_lead_creation,
        voc_date,
        opp_type,
        null AS is_working
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_voc"
    UNION ALL
    SELECT DISTINCT
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        industry_bucket,
        channel_bucket,
        target_account,
        'No' AS is_hand_raiser,
        offer_asset_name_lead_creation,
        closing_date,
        opp_type,
        null AS is_working
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_closing"
    UNION ALL
    SELECT DISTINCT
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        industry_bucket,
        channel_bucket,
        target_account,
        'No' AS is_hand_raiser,
        offer_asset_name_lead_creation,
        won_date,
        opp_type,
        null AS is_working
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_won"
    UNION ALL
    SELECT DISTINCT
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        industry_bucket,
        channel_bucket,
        target_account,
        'No' AS is_hand_raiser,
        offer_asset_name_lead_creation,
        lost_date,
        opp_type,
        null AS is_working
    FROM "acton"."dbt_actonmarketing"."funnel_report_all_time_lost"

)

SELECT DISTINCT
    CASE
        WHEN global_region IS null THEN 'blank'
        ELSE global_region
    END AS global_region,
    CASE
        WHEN company_size_rev IS null THEN 'blank'
        ELSE company_size_rev
    END AS company_size_rev,
    CASE
        WHEN lead_source IS null THEN 'blank'
        ELSE lead_source
    END AS lead_source,
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
        WHEN offer_asset_name_lead_creation IS null THEN 'blank'
        ELSE offer_asset_name_lead_creation
    END AS offer_asset_name_lead_creation,
    CASE
        WHEN opp_type IS null THEN 'blank'
        ELSE opp_type
    END AS opp_type,
    CASE
        WHEN is_working IS null THEN 'No'
        ELSE is_working
    END AS is_working,
    is_hand_raiser,
    CASE
        WHEN target_account IS null THEN false
        ELSE target_account
    END AS target_account,
    date
FROM base
  );