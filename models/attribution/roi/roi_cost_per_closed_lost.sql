{{ config(materialized='table') }}

WITH campaign_base AS (

    SELECT
        COALESCE(actual_cost,budgeted_cost) AS cost,
        campaign_type,
        campaign_start_date
    FROM {{ref('campaign_source_xf')}}

), opp_base AS (

    SELECT DISTINCT
    --IDs
        opportunity_id,

    --Dates
        created_date,
        discovery_date,
        date_reached_demo_confirmed,
        date_reached_demo_complete,
        negotiation_date,
        closing_date,
        close_date,

    --Flags
        is_closed,
        is_won,

    --Attribution
        channel_bucket,
        channel_bucket_details
    FROM {{ref('opp_source_xf')}}
    WHERE opp_source_xf.close_date IS NOT null
    AND opp_source_xf.stage_name IN ('Closed – Lost No Resources / Budget','Closed – Lost Not Ready / No Decision','Closed – Lost Product Deficiency','Closed - Lost to Competitor', 'Closed Lost')


), final AS (

    SELECT
        campaign_start_date,
        close_date,
        opp_base.channel_bucket,
        SUM(cost) AS total_cost,
        COUNT(DISTINCT opportunity_id) AS closed_lost
    FROM opp_base
    LEFT JOIN campaign_base
        ON opp_base.channel_bucket=campaign_base.campaign_type
    WHERE cost IS NOT null
    GROUP BY 1,2,3
    
)

SELECT *
FROM final