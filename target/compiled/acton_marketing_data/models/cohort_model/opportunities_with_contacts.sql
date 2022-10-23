

WITH person_base AS (

    SELECT
        person_id,
        email,
        is_hand_raiser,
        person_owner_id AS owner_id,
        channel_lead_creation,
        medium_lead_creation,
        source_lead_creation,
        lead_source,
        created_date,
        marketing_created_date,
        mql_most_recent_date AS mql_created_date,
        working_date,
        person_status,
        company_size_rev,
        global_region,
        segment,
        lean_data_account_id AS account_id,
        channel_bucket,
        industry_bucket,
        industry,
        offer_asset_name_lead_creation
    FROM "acton"."dbt_actonmarketing"."person_source_xf"
    --WHERE marketing_created_date >= '2021-01-01'
)

SELECT DISTINCT
    person_base.person_id,
    person_base.email,
    person_base.is_hand_raiser,
    person_base.channel_bucket,
    person_base.owner_id,
    person_base.channel_lead_creation,
    person_base.medium_lead_creation,
    person_base.source_lead_creation,
    person_base.lead_source,
    person_base.created_date AS person_created_date,
    person_base.marketing_created_date,
    person_base.mql_created_date,
    person_base.working_date,
    person_base.company_size_rev,
    person_base.global_region,
    person_base.segment,
    person_base.account_id,
    person_base.person_status,
    person_base.industry,
    person_base.offer_asset_name_lead_creation AS person_offer_asset_name_lead_creation,
    person_base.industry_bucket,
    -- account_base.is_current_customer, 
    -- account_base.account_name,
    -- account_base.account_owner_name,
    -- account_base.account_csm_name,
   -- account_base.
    opp_base.is_current_customer,
    opp_base.opportunity_id,
    opp_base.opportunity_name,
    opp_base.is_won,
    opp_base.created_date AS opp_created_date,
    opp_base.discovery_date,
    opp_base.demo_date,
    opp_base.voc_date,
    opp_base.closing_date,
    opp_base.implement_date,
    opp_base.close_date,
    opp_base.stage_name,
    opp_base.acv_deal_size_usd AS acv,
    opp_base.opp_lead_source,
    opp_base.type,
    opp_base.is_closed,
    opp_base.segment AS opp_segment,
    opp_base.account_global_region,
    opp_base.company_size_rev AS opp_company_size_rev,
    opp_base.industry AS opp_industry,
    opp_base.industry_bucket AS opp_industry_bucket,
    opp_base.channel_bucket AS opp_channel_bucket,
    opp_base.opp_offer_asset_name_lead_creation,
    CASE 
        WHEN mql_created_date IS NOT null AND email NOT LIKE '%act-on%' THEN 1
        ELSE 0
    END AS is_mql,
    CASE 
        WHEN working_date IS NOT null AND email NOT LIKE '%act-on%' AND person_status NOT IN ('Current Customer','Partner','Bad Data','No Fit') THEN 1
        ELSE 0
    END AS is_sal,
    CASE
        WHEN opp_base.created_date IS NOT null AND opp_base.stage_name NOT IN ('Closed - Duplicate','Closed - Admin Remove') THEN 1
        ELSE 0
    END AS is_sql,
    CASE
        WHEN opp_base.discovery_date IS NOT null AND opp_base.stage_name NOT IN ('Closed - Duplicate','Closed - Admin Remove') THEN 1
        ELSE 0
    END AS is_sqo,
    CASE
        WHEN opp_base.demo_date IS NOT null AND opp_base.stage_name NOT IN ('Closed - Duplicate','Closed - Admin Removed','SQL','Discovery') THEN 1
        ELSE 0
    END AS is_demo,
    CASE
        WHEN opp_base.voc_date IS NOT null AND opp_base.stage_name NOT IN ('Closed - Duplicate','Closed - Admin Removed','SQL','Discovery','Demo') THEN 1
        ELSE 0
    END AS is_voc,
    CASE
        WHEN opp_base.closing_date IS NOT null AND opp_base.stage_name NOT IN ('Closed - Duplicate','Closed - Admin Removed','SQL','Discovery','Demo','VOC/Negotiate') THEN 1
        ELSE 0
    END AS is_closing,
    CASE
        WHEN opp_base.is_closed = true AND opp_base.is_won = false THEN 1
        ELSE 0
    END AS is_cl,
    CASE
        WHEN opp_base.is_closed = true AND opp_base.is_won = true THEN 1
        ELSE 0
    END AS is_cw
FROM person_base
FULL JOIN "acton"."dbt_actonmarketing"."opp_source_xf" AS opp_base ON
person_base.account_id=opp_base.account_id