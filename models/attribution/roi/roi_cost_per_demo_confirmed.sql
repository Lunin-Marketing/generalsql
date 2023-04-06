{{ config(materialized='table') }}

WITH campaign_base AS (

    SELECT DISTINCT
        campaign_type,
        DATE_TRUNC('month',campaign_start_date) AS campaign_start_date,
        CASE
            WHEN actual_cost IS null OR actual_cost = 0 THEN budgeted_cost
            ELSE actual_cost
        END AS cost
    FROM {{ref('campaign_source_xf')}}
    WHERE cost IS NOT null 
    ORDER BY 1,2 DESC

), campaign_final AS (

    SELECT DISTINCT
        campaign_type,
        campaign_start_date,
        SUM(cost) AS cost
    FROM campaign_base
    GROUP BY 1,2
    
), opp_base AS (

    SELECT DISTINCT
    --IDs
        opportunity_id,

    --Dates
        opp_created_date,
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
        channel_bucket_lt,
        channel_bucket_details,
        campaign_type,
        campaign_member_first_responded_date,
        DATE_TRUNC('month',campaign_source_xf.campaign_start_date) AS campaign_start_date
    FROM {{ref('opportunities_with_contacts')}}
    LEFT JOIN {{ref('campaign_member_source_xf')}}
        ON opportunities_with_contacts.person_id=campaign_member_source_xf.lead_or_contact_id
    LEFT JOIN {{ref('campaign_source_xf')}}
        ON campaign_member_source_xf.campaign_id=campaign_source_xf.campaign_id
    WHERE date_reached_demo_confirmed IS NOT null
        AND stage_name NOT IN ('Closed - Duplicate','Closed - Admin Removed','SQL','Discovery','Implement')

), campaign_member_intermediate AS (

    SELECT DISTINCT
        opportunity_id,
        campaign_type,
        campaign_start_date,
        ROW_NUMBER() OVER (PARTITION BY opportunity_id ORDER BY campaign_member_first_responded_date DESC ) AS campaign_order
    FROM opp_base
    WHERE date_reached_demo_confirmed >= campaign_member_first_responded_date

), campaign_member_final AS (

    SELECT DISTINCT
        opportunity_id,
        campaign_type,
        campaign_start_date
    FROM campaign_member_intermediate
    WHERE campaign_order = 1

), opp_final AS (

    SELECT DISTINCT
        opportunity_id,
        opp_base.channel_bucket_lt,
        DATE_TRUNC('month',start_date) AS cost_date
    FROM opp_base
    LEFT JOIN {{ref('demand_gen_costs_fy23')}}
        ON opp_base.channel_bucket_lt=demand_gen_costs_fy23.channel_bucket
        AND DATE_TRUNC('month',opp_base.date_reached_demo_confirmed)=DATE_TRUNC('month',demand_gen_costs_fy23.start_date)

), member_opp_combined AS (

    SELECT DISTINCT
        opportunity_id,
        campaign_type,
        campaign_start_date
    FROM campaign_member_final
    UNION ALL
    SELECT DISTINCT
        opportunity_id,
        channel_bucket_lt,
        cost_date
    FROM opp_final

), opp_cost_final AS (

    SELECT DISTINCT
        opp_base.channel_bucket_lt,
        DATE_TRUNC('month',start_date) AS cost_date,
        SUM(cost)/COUNT(DISTINCT opportunity_id) AS total_cost
    FROM opp_base
    LEFT JOIN {{ref('demand_gen_costs_fy23')}}
        ON opp_base.channel_bucket_lt=demand_gen_costs_fy23.channel_bucket
    WHERE cost IS NOT null
    GROUP BY 1,2

), cost_final AS (

    SELECT DISTINCT
        campaign_type,
        campaign_start_date,
        cost
    FROM campaign_final
    UNION ALL
    SELECT DISTINCT
        channel_bucket_lt,
        cost_date,
        total_cost
    FROM opp_cost_final

), final AS (

    SELECT DISTINCT
        cost_final.campaign_start_date,
        cost_final.campaign_type,
        cost AS total_cost,
        COUNT(DISTINCT opportunity_id) AS demo_confirmed
    FROM member_opp_combined
    LEFT JOIN cost_final 
        ON member_opp_combined.campaign_type=cost_final.campaign_type
        AND member_opp_combined.campaign_start_date=cost_final.campaign_start_date
    GROUP BY 1,2,3

)

SELECT *
FROM final
ORDER BY 1,2 DESC