{{ config(materialized='table') }}

WITH base AS (

SELECT *
FROM {{ source('salesforce', 'campaign') }}

), final AS (

    SELECT
        id AS campaign_id,
        name AS campaign_name, 
        type AS campaign_type,
        status AS campaign_status,
        DATE_TRUNC('day',base.start_date)::Date AS campaign_start_date,
        DATE_TRUNC('day',base.end_date)::Date AS campaign_end_date,
        parent_id AS parent_campaign_id,
        expected_revenue,
        budgeted_cost,
        actual_cost,
        expected_response,
        owner_id AS campaign_owner_id,
        is_active AS is_active_campaign,
        is_deleted AS is_deleted_campaign
    FROM base

)

SELECT *
FROM final