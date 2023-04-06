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
    
), person_base AS (

    SELECT DISTINCT
    --IDs
        person_id,

    --Dates
        created_date,
        marketing_created_date,
        mql_most_recent_date,

    --Attribution
        channel_bucket_lt,
        campaign_type,
        campaign_member_first_responded_date,
        DATE_TRUNC('month',campaign_source_xf.campaign_start_date) AS campaign_start_date
    FROM {{ref('person_source_xf')}}
    LEFT JOIN {{ref('campaign_member_source_xf')}}
        ON person_source_xf.person_id=campaign_member_source_xf.lead_or_contact_id
    LEFT JOIN {{ref('campaign_source_xf')}}
        ON campaign_member_source_xf.campaign_id=campaign_source_xf.campaign_id
    WHERE mql_most_recent_date IS NOT null
        AND person_owner_id != '00Ga0000003Nugr' -- AO-Fake Leads
        AND person_source_xf.email NOT LIKE '%act-on.com'

), campaign_member_intermediate AS (

    SELECT DISTINCT
        person_id,
        campaign_type,
        campaign_start_date,
        ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY campaign_member_first_responded_date DESC ) AS campaign_order
    FROM person_base
    WHERE mql_most_recent_date >= campaign_member_first_responded_date

),campaign_member_final AS (

    SELECT DISTINCT
        person_id,
        campaign_type,
        campaign_start_date
    FROM campaign_member_intermediate
    WHERE campaign_order = 1

), person_cost_final AS (

    SELECT DISTINCT
        person_base.channel_bucket_lt,
        DATE_TRUNC('month',start_date) AS cost_date,
        SUM(cost)/COUNT(DISTINCT person_id) AS total_cost
    FROM person_base
    LEFT JOIN {{ref('demand_gen_costs_fy23')}}
        ON person_base.channel_bucket_lt=demand_gen_costs_fy23.channel_bucket
    WHERE cost IS NOT null
    GROUP BY 1,2

), person_final AS (

    SELECT DISTINCT
        person_id,
        person_base.channel_bucket_lt,
        DATE_TRUNC('month',start_date) AS cost_date
    FROM person_base
    LEFT JOIN {{ref('demand_gen_costs_fy23')}}
        ON person_base.channel_bucket_lt=demand_gen_costs_fy23.channel_bucket
        AND DATE_TRUNC('month',person_base.mql_most_recent_date)=DATE_TRUNC('month',demand_gen_costs_fy23.start_date)

), member_person_combined AS (

    SELECT DISTINCT
        person_id,
        campaign_type,
        campaign_start_date
    FROM campaign_member_final
    UNION ALL
    SELECT DISTINCT
        person_id,
        channel_bucket_lt,
        cost_date
    FROM person_final

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
    FROM person_cost_final

), final AS (

    SELECT DISTINCT
        cost_final.campaign_start_date,
        cost_final.campaign_type,
        cost AS total_cost,
        COUNT(DISTINCT person_id) AS mqls
    FROM member_person_combined
    LEFT JOIN cost_final 
        ON member_person_combined.campaign_type=cost_final.campaign_type
        AND member_person_combined.campaign_start_date=cost_final.campaign_start_date
    GROUP BY 1,2,3

)

SELECT *
FROM final
ORDER BY 1,2 DESC