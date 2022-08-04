{{ config(materialized='table') }}

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
    FROM {{ref('opportunities_with_contacts')}}
    WHERE type='New Business'

), final AS (

    SELECT
        person_id,
        marketing_created_date,
        opp_created_date,
        global_region,
        company_size_rev,
        lead_source,
        segment,
        industry,
        channel_bucket,
        {{ dbt_utils.datediff("marketing_created_date","opp_created_date",'day')}} AS lead_to_opp_velocity
    FROM base
    WHERE opp_created_date >= marketing_created_date
)

SELECT
    global_region,
    company_size_rev,
    lead_source,
    segment,
    industry,
    channel_bucket,
    marketing_created_date,
    AVG(lead_to_opp_velocity)
FROM final
GROUP BY 1,2,3,4,5,6,7