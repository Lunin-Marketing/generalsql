

WITH base AS (

    SELECT *
    FROM "acton"."dbt_actonmarketing"."opportunities_with_contacts"

), intermediate AS (

    SELECT
        person_id,
        email,
        opportunity_id,
        owner_id AS person_owner_id,
        company_size_rev,
        global_region,
        segment,
        channel_bucket,
        industry,
        industry_bucket,
        lead_source,
        person_created_date,
        marketing_created_date::Date,
        mql_created_date::Date,
        working_date AS sal_created_date,
        person_status,
        opp_created_date::Date AS sql_date,
        discovery_date::Date AS sqo_date,
        demo_date::Date,
        voc_date::Date,
        closing_date::Date,
        close_date::Date,
        CASE
            WHEN is_won = true THEN close_date::Date
            ELSE null
        END AS cw_date,
        CASE 
            WHEN is_won = false AND is_closed = true THEN close_date::Date
            ELSE null
        END AS cl_date,
        is_won,
        acv,
        channel_lead_creation,
        medium_lead_creation,
        source_lead_creation,
        person_offer_asset_name_lead_creation,
        stage_name,
        opp_lead_source,
        opp_segment,
        account_global_region,
        opp_company_size_rev,
        opp_industry,
        opp_industry_bucket,
        opp_channel_bucket,
        opp_offer_asset_name_lead_creation,
        type AS opp_type,
        is_hand_raiser,
        is_current_customer,
        is_mql,
        CASE
            WHEN is_mql = 1 THEN person_id
            ELSE null
        END AS mql_id,
        is_sal,
        CASE
            WHEN is_sal = 1 THEN person_id
            ELSE null
        END AS sal_id,
        is_sql,
        CASE
            WHEN is_sql = 1 THEN opportunity_id
            ELSE null
        END AS sql_id,
        is_sqo,
        CASE
            WHEN is_sqo = 1 THEN opportunity_id
            ELSE null
        END AS sqo_id,
        is_demo,
        CASE
            WHEN is_demo = 1 THEN opportunity_id
            ELSE null
        END AS demo_id,
        is_voc,
        CASE
            WHEN is_voc = 1 THEN opportunity_id
            ELSE null
        END AS voc_id,
        is_closing,
        CASE
            WHEN is_closing = 1 THEN opportunity_id
            ELSE null
        END AS closing_id,
        is_cl,
        CASE
            WHEN is_cl = 1 THEN opportunity_id
            ELSE null
        END AS cl_id,
        is_cw,
        CASE
            WHEN is_cw = 1 THEN opportunity_id
            ELSE null
        END AS cw_id
    FROM base

), final AS (

    SELECT 
        intermediate.*,
        CASE 
            WHEN mql_created_date>=marketing_created_date THEN 
        ((mql_created_date)::date - (marketing_created_date)::date)
     
            ELSE 0 
        END AS days_to_mql,
        CASE 
            WHEN sal_created_date>=mql_created_date THEN 
        ((sal_created_date)::date - (mql_created_date)::date)
     
            ELSE 0 
        END AS  days_to_sal,
        CASE 
            WHEN sql_date>=sal_created_date THEN 
        ((sql_date)::date - (sal_created_date)::date)
     
            ELSE 0 
        END AS  days_to_sql,
        CASE 
            WHEN sqo_date>=sql_date THEN 
        ((sqo_date)::date - (sql_date)::date)
     
            ELSE 0 
        END AS  days_to_sqo,
        CASE 
            WHEN cw_date>=sqo_date THEN 
        ((cw_date)::date - (sqo_date)::date)
     
            ELSE 0 
        END AS  days_to_won,
        CASE 
            WHEN cl_date>=sqo_date THEN 
        ((cl_date)::date - (sqo_date)::date)
     
            ELSE 0 
        END AS  days_to_closed_lost
    FROM intermediate

)

SELECT *
FROM final