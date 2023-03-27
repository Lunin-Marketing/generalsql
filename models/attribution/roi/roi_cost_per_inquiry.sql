{{ config(materialized='table') }}

WITH campaign_base AS (

    SELECT
        COALESCE(actual_cost,budgeted_cost) AS cost,
        campaign_type,
        campaign_start_date
    FROM {{ref('campaign_source_xf')}}

), person_base AS (

    SELECT DISTINCT
    --IDs
        person_id,

    --Dates
        created_date,
        marketing_created_date,
        mql_most_recent_date,

    --Attribution
        channel_bucket,
        channel_bucket_details
    FROM {{ref('person_source_xf')}}
    WHERE marketing_created_date IS NOT null

), final AS (

    SELECT
        campaign_start_date,
        marketing_created_date,
        channel_bucket,
        SUM(cost) AS total_cost,
        COUNT(DISTINCT person_id) AS inquiries
    FROM person_base
    LEFT JOIN campaign_base
        ON person_base.channel_bucket=campaign_base.campaign_type
    WHERE cost IS NOT null
    GROUP BY 1,2,3
    
)

SELECT *
FROM final