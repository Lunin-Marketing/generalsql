{{ config(materialized='table') }}

SELECT
--IDs
    person_id,
    mql_id,
    sal_id,
    opportunity_id,
    sql_id,
    sqo_id,
    demo_id,
    voc_id,
    closing_id,
    cl_id,
    cw_id,

--person fields
    marketing_created_date,
    mql_created_date,
    sal_created_date,
    person_status,
    person_owner_id,

--opp fields
    stage_name,
    opp_type,
    sql_date,
    sqo_date,
    demo_date,
    voc_date,
    closing_date,
    cw_date,
    cl_date,

--Info
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
        WHEN person_offer_asset_name_lead_creation IS null THEN 'blank'
        ELSE person_offer_asset_name_lead_creation
    END AS offer_asset_name_lead_creation,
    CASE
        WHEN account_global_region IS null THEN 'blank'
        ELSE account_global_region
    END AS account_global_region,
    CASE
        WHEN opp_company_size_rev IS null THEN 'blank'
        ELSE opp_company_size_rev
    END AS opp_company_size_rev,
    CASE
        WHEN opp_lead_source IS null THEN 'blank'
        ELSE opp_lead_source
    END AS opp_lead_source,
    CASE
        WHEN opp_segment IS null THEN 'blank'
        ELSE opp_segment
    END AS opp_segment,
    CASE
        WHEN opp_industry IS null THEN 'blank'
        ELSE opp_industry
    END AS opp_industry,
    CASE
        WHEN opp_industry_bucket IS null THEN 'blank'
        ELSE opp_industry_bucket
    END AS opp_industry_bucket,
    CASE
        WHEN opp_channel_bucket IS null THEN 'blank'
        ELSE opp_channel_bucket
    END AS opp_channel_bucket,
    CASE
        WHEN opp_offer_asset_name_lead_creation IS null THEN 'blank'
        ELSE opp_offer_asset_name_lead_creation
    END AS opp_offer_asset_name_lead_creation,

--Flags
    is_hand_raiser,
    is_current_customer
FROM {{ref('lead_to_cw_cohort')}}