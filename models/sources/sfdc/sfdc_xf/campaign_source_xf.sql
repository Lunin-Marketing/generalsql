{{ config(materialized='table') }}

WITH base AS (

SELECT *
FROM {{ source('salesforce', 'campaign') }}

), final AS (

    SELECT
    --Campaign Info
        base.id AS campaign_id,
        base.name AS campaign_name, 
        base.type AS campaign_type,
        base.status AS campaign_status,
        DATE_TRUNC('day',base.start_date)::Date AS campaign_start_date,
        DATE_TRUNC('day',base.end_date)::Date AS campaign_end_date,
        base.expected_revenue,
        base.budgeted_cost,
        base.actual_cost,
        base.expected_response,
        base.owner_id AS campaign_owner_id,
        base.is_active AS is_active_campaign,
        base.is_deleted AS is_deleted_campaign,

    --Parent Campaign Info
        base.parent_id AS parent_campaign_id,
        parent.name AS parent_campaign_name,
        parent.type AS parent_campaign_type,
        parent.status AS parent_campaign_status,
        DATE_TRUNC('day',parent.start_date)::Date AS parent_campaign_start_date,
        DATE_TRUNC('day',parent.end_date)::Date AS parent_campaign_end_date,
        parent.expected_revenue AS parent_expected_revenue,
        parent.budgeted_cost AS parent_budgeted_cost,
        parent.actual_cost AS parent_actual_cost,
        parent.expected_response AS parent_expected_reponse,
        parent.owner_id AS parent_campaign_owner_id,
        parent.is_active AS is_active_parent_campaign
    FROM base
    LEFT JOIN base parent ON 
    base.parent_id=parent.id

)

SELECT *
FROM final