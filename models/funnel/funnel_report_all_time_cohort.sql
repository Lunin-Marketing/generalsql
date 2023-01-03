{{ config(materialized='table') }}

WITH base AS (

    SELECT
    --person fields
        person_id,
        marketing_created_date,
        mql_created_date,
        sal_created_date,
        person_status,
        email,
        person_owner_id,

    --opp fields
        opportunity_id,
        stage_name,
        opp_type,
        sql_date,
        sqo_date,
        demo_date,
        voc_date,
        closing_date,
        close_date,
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
        is_mql,
        is_sal,
        is_sql,
        is_sqo,
        is_demo,
        is_voc,
        is_closing,
        is_cl,
        is_cw,
        is_won
    FROM {{ref('lead_to_cw_cohort')}}

)

SELECT
--ID Fields
    person_id,
    CASE
        WHEN is_mql = 1 THEN person_id
        ELSE null
    END AS mql_id,
    CASE
        WHEN is_sal = 1 THEN person_id
        ELSE null
    END AS sal_id,
    CASE
        WHEN is_sql = 1 THEN opportunity_id
        ELSE null
    END AS sql_id,
    CASE
        WHEN is_sqo = 1 THEN opportunity_id
        ELSE null
    END AS sqo_id,
    CASE
        WHEN is_demo = 1 THEN opportunity_id
        ELSE null
    END AS demo_id,
    CASE
        WHEN is_voc = 1 THEN opportunity_id
        ELSE null
    END AS voc_id,
    CASE
        WHEN is_closing = 1 THEN opportunity_id
        ELSE null
    END AS closing_id,
    CASE
        WHEN is_cl = 1 THEN opportunity_id
        ELSE null
    END AS cl_id,
    CASE
        WHEN is_cw = 1 THEN opportunity_id
        ELSE null
    END AS cw_id,

--person fields
        marketing_created_date,
        mql_created_date,
        sal_created_date,
        person_status,

--opp fields
        opportunity_id,
        opp_type,
        stage_name,
        sql_date,
        sqo_date,
        demo_date,
        voc_date,
        closing_date,
        close_date,
        cw_date,
        cl_date,

-- person data
    global_region,
    company_size_rev,
    lead_source,
    segment,
    industry,
    industry_bucket,
    channel_bucket,
    offer_asset_name_lead_creation,

-- opp data
    account_global_region,
    opp_company_size_rev,
    opp_lead_source,
    opp_segment,
    opp_industry,
    opp_industry_bucket,
    opp_channel_bucket,
    opp_offer_asset_name_lead_creation,

--Flags
    is_hand_raiser,
    is_mql,
    is_sal,
    is_sql,
    is_sqo,
    is_demo,
    is_voc,
    is_closing,
    is_cl,
    is_cw,
    is_won
FROM base
