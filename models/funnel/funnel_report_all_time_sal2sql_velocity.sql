{{ config(materialized='table') }}

WITH base AS (

    SELECT
        person_id,
        working_date,
        opportunity_id,
        global_region,
        company_size_rev,
        lead_source,
        segment,
        industry,
        channel_bucket,
        target_account,
        industry_bucket,
        opp_created_date,
        account_global_region,
        opp_company_size_rev,
        opp_lead_source,
        opp_segment,
        opp_industry,
        opp_channel_bucket,
        opp_industry_bucket
    FROM {{ref('opportunities_with_contacts')}}
    WHERE type='New Business'

), final AS (

    SELECT
        person_id,
        working_date,
        opp_created_date,
        target_account,
        account_global_region,
        opp_company_size_rev,
        opp_lead_source,
        opp_segment,
        opp_industry,
        opp_channel_bucket,
        opp_industry_bucket,
        {{ datediff("working_date","opp_created_date",'day')}} AS sal2sql_velocity
    FROM base
    WHERE opp_created_date >= working_date
)

SELECT
    account_global_region,
    opp_company_size_rev,
    opp_lead_source,
    opp_segment,
    opp_industry,
    target_account,
    opp_channel_bucket,
    opp_industry_bucket,
    opp_created_date,
    sal2sql_velocity
FROM final