{{ config(materialized='table') }}

WITH base AS (

    SELECT DISTINCT
        campaign_type_date_base."CAMPAIGN_TYPE" AS campaign_type,
        "Month"::Date AS month,
        CASE
            WHEN actual_cost IS null OR actual_cost = 0 THEN budgeted_cost
            ELSE actual_cost
        END AS cost
    FROM {{source('common','campaign_type_date_base')}}
    LEFT JOIN {{ref('campaign_source_xf')}}
        ON campaign_type_date_base."CAMPAIGN_TYPE"=campaign_source_xf.campaign_type
        AND campaign_type_date_base."Month"=DATE_TRUNC('month',campaign_source_xf.campaign_start_date)
    UNION ALL
    SELECT DISTINCT
        channel_bucket,
        DATE_TRUNC('month',demand_gen_costs_fy23.start_date) AS date,
        cost
    FROM {{ref('demand_gen_costs_fy23')}}
)

SELECT 
    campaign_type,
    month,
    SUM(cost) AS total_cost
FROM base
GROUP BY 1,2