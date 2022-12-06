{{ config(materialized='table') }}

WITH base AS (

    SELECT *
    FROM {{ref('opportunities_with_contactroles')}}

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
        channel_bucket_details,
        industry,
        industry_bucket,
        lead_source,
        person_created_date,
        marketing_created_date::Date,
        person_status,
        is_won,
        acv,
        person_campaign_lead_creation,
        person_channel_lead_creation,
        person_medium_lead_creation,
        person_source_lead_creation,
        person_subchannel_lead_creation,
        person_offer_asset_subtype_lead_creation,
        person_offer_asset_topic_lead_creation,
        person_offer_asset_type_lead_creation,
        person_offer_asset_name_lead_creation,
        stage_name,
        opp_lead_source,
        opp_segment,
        account_global_region,
        opp_company_size_rev,
        opp_industry,
        opp_industry_bucket,
        opp_channel_bucket,
        opp_channel_bucket_details,
        opp_channel_lead_creation,
        opp_medium_lead_creation,
        opp_source_lead_creation,
        opp_offer_asset_name_lead_creation,
        opp_offer_asset_topic_lead_creation,
        opp_offer_asset_type_lead_creation,
        opp_subchannel_lead_creation,
        opp_offer_asset_subtype_lead_creation,
        type AS opp_type,
        is_hand_raiser,
        is_current_customer,
        is_mql,
        CASE
            WHEN is_mql = 1 THEN person_id
            ELSE null
        END AS mql_id,
        CASE
            WHEN is_mql = 1 THEN mql_created_date::Date
            ELSE null
        END AS mql_created_date,
        is_sal,
        CASE
            WHEN is_sal = 1 THEN person_id
            ELSE null
        END AS sal_id,
        CASE
            WHEN is_sal = 1 THEN working_date::Date
            ELSE null
        END AS sal_created_date,
        is_sql,
        CASE
            WHEN is_sql = 1 THEN opportunity_id
            ELSE null
        END AS sql_id,
        CASE
            WHEN is_sql = 1 THEN opp_created_date::Date
            ELSE null
        END AS sql_date,
        is_sqo,
        CASE
            WHEN is_sqo = 1 THEN opportunity_id
            ELSE null
        END AS sqo_id,
        CASE
            WHEN is_sqo = 1 THEN discovery_date::Date
            ELSE null
        END AS sqo_date,
        is_demo,
        CASE
            WHEN is_demo = 1 THEN opportunity_id
            ELSE null
        END AS demo_id,
        CASE
            WHEN is_demo = 1 THEN demo_date::Date
            ELSE null
        END AS demo_date,
        is_voc,
        CASE
            WHEN is_voc = 1 THEN opportunity_id
            ELSE null
        END AS voc_id,
        CASE
            WHEN is_voc = 1 THEN voc_date::Date
            ELSE null
        END AS voc_date,
        is_closing,
        CASE
            WHEN is_closing = 1 THEN opportunity_id
            ELSE null
        END AS closing_id,
        CASE
            WHEN is_closing = 1 THEN closing_date::Date
            ELSE null
        END AS closing_date,
        is_cl,
        CASE
            WHEN is_cl = 1 THEN opportunity_id
            ELSE null
        END AS cl_id,
        CASE
            WHEN is_cl = 1 AND is_won = false AND is_closed = true THEN close_date::Date
            ELSE null
        END AS cl_date,
        is_cw,
        CASE
            WHEN is_cw = 1 AND is_won = true THEN close_date::Date
            ELSE null
        END AS cw_date,
        CASE
            WHEN is_cw = 1 THEN opportunity_id
            ELSE null
        END AS cw_id
    FROM base

), final AS (

    SELECT 
        intermediate.*,
        CASE 
            WHEN mql_created_date>=marketing_created_date THEN {{ dbt.datediff("marketing_created_date","mql_created_date",'day') }} 
            ELSE 0 
        END AS days_to_mql,
        CASE 
            WHEN sal_created_date>=mql_created_date THEN {{ dbt.datediff("mql_created_date","sal_created_date",'day') }} 
            ELSE 0 
        END AS  days_to_sal,
        CASE 
            WHEN sql_date>=sal_created_date THEN {{ dbt.datediff("sal_created_date","sql_date",'day') }} 
            ELSE 0 
        END AS  days_to_sql,
        CASE 
            WHEN sqo_date>=sql_date THEN {{ dbt.datediff("sql_date","sqo_date",'day') }} 
            ELSE 0 
        END AS  days_to_sqo,
        CASE 
            WHEN cw_date>=sqo_date THEN {{ dbt.datediff("sqo_date","cw_date",'day') }} 
            ELSE 0 
        END AS  days_to_won,
        CASE 
            WHEN cl_date>=sqo_date THEN {{ dbt.datediff("sqo_date","cl_date",'day') }} 
            ELSE 0 
        END AS  days_to_closed_lost
    FROM intermediate

)

SELECT *
FROM final  