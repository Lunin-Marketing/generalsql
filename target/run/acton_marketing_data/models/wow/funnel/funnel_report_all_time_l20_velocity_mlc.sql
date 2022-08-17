

  create  table "acton"."dbt_actonmarketing"."funnel_report_all_time_l20_velocity_mlc__dbt_tmp"
  as (
    

WITH base AS (

    SELECT
        person_id,
        marketing_created_date,
        opportunity_id,
        global_region,
        company_size_rev,
        lead_source,
        segment,
        industry,
        channel_bucket,
        opp_created_date,
        account_global_region,
        opp_company_size_rev,
        opp_lead_source,
        opp_segment,
        opp_industry,
        opp_channel_bucket
    FROM "acton"."dbt_actonmarketing"."opportunities_with_contacts"
    WHERE type='New Business'

), final AS (

    SELECT
        person_id,
        marketing_created_date,
        opp_created_date,
        account_global_region,
        opp_company_size_rev,
        opp_lead_source,
        opp_segment,
        opp_industry,
        opp_channel_bucket,
        

    
        ((opp_created_date)::date - (marketing_created_date)::date)
    

 AS lead_to_opp_velocity
    FROM base
    WHERE opp_created_date >= marketing_created_date
)

SELECT
    account_global_region,
    opp_company_size_rev,
    opp_lead_source,
    opp_segment,
    opp_industry,
    opp_channel_bucket,
    marketing_created_date,
    lead_to_opp_velocity
FROM final
  );