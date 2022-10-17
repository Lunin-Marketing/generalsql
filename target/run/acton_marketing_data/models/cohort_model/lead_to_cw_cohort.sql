

  create  table "acton"."dbt_actonmarketing"."lead_to_cw_cohort__dbt_tmp"
  as (
    

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
        opp_created_date::Date,
        discovery_date::Date,
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
        is_sal,
        is_sql,
        is_sqo,
        is_demo,
        is_voc,
        is_closing,
        is_cl,
        is_cw
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
            WHEN opp_created_date>=sal_created_date THEN 
        ((opp_created_date)::date - (sal_created_date)::date)
     
            ELSE 0 
        END AS  days_to_sql,
        CASE 
            WHEN discovery_date>=opp_created_date THEN 
        ((discovery_date)::date - (opp_created_date)::date)
     
            ELSE 0 
        END AS  days_to_sqo,
        CASE 
            WHEN cw_date>=discovery_date THEN 
        ((cw_date)::date - (discovery_date)::date)
     
            ELSE 0 
        END AS  days_to_won,
        CASE 
            WHEN cl_date>=discovery_date THEN 
        ((cl_date)::date - (discovery_date)::date)
     
            ELSE 0 
        END AS  days_to_closed_lost
    FROM intermediate

)

SELECT *
FROM final
  );