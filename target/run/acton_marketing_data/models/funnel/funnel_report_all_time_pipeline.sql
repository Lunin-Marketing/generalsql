

  create  table "acton"."dbt_actonmarketing"."funnel_report_all_time_pipeline__dbt_tmp"
  as (
    

WITH base AS (

    SELECT DISTINCT
        sqo_source_xf.opportunity_id AS sqo_id,
        CONCAT('https://acton.my.salesforce.com/',sqo_source_xf.opportunity_id) AS sqo_url,
        sqo_source_xf.discovery_date AS sqo_date,
        sqo_source_xf.created_date AS created_date,
        account_name,
        opportunity_name,
        stage_name,
        type AS opp_type,
        owner_name,
        sdr_name,
        close_date,
        acv,
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
    FROM "acton"."dbt_actonmarketing"."sqo_source_xf"

) , final AS (

    SELECT DISTINCT
        sqo_id,
        sqo_date,
        created_date,
        opportunity_name,
        stage_name,
        opp_type,
        acv,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        industry_bucket,
        channel_bucket,
        CASE
            WHEN stage_name = 'Discovery' THEN '1.SQO'
            WHEN stage_name = 'Demo' THEN '2.Demo'
            WHEN stage_name = 'VOC/Negotiate' THEN '3.VOC'
            WHEN stage_name = 'Closing' THEN '4.Closing'
            WHEN stage_name = 'Implement' THEN '5.Closed Won'
            WHEN LOWER(stage_name) LIKE '%lost%' THEN '6.Closed Lost'
            WHEN stage_name = 'Not Renewed' THEN '7.Not Renewed'
        END AS current_stage
    FROM base

)

SELECT *
FROM final
  );