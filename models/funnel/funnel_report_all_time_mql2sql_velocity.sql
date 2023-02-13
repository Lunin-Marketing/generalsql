{{ config(materialized='table') }}

WITH base AS (

    SELECT
        person_id,
        mql_created_date AS mql_date,
        opportunity_id,
        global_region,
        company_size_rev,
        lead_source,
        segment,
        industry,
        channel_bucket,
        target_account,
        industry_bucket,
        opp_created_date AS sql_date,
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
        mql_date,
        sql_date,
        opp_company_size_rev,
        opp_lead_source,
        account_global_region,
        opp_segment,
        target_account,
        opp_industry,
        opp_channel_bucket,
        opp_industry_bucket,
        {{ datediff("mql_date","sql_date",'day')}} AS mql2sql_velocity
    FROM base
    WHERE sql_date >= mql_date
)

SELECT
    account_global_region,
    opp_company_size_rev,
    opp_lead_source,
    opp_segment,
    opp_industry,
    opp_channel_bucket,
    target_account,
    opp_industry_bucket,
    sql_date,
    mql2sql_velocity
FROM final