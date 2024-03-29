{{ config(materialized='table') }}

WITH base AS (

SELECT *
FROM {{ source('salesforce', 'campaign_influence') }}

), final AS (

    SELECT  
        id AS influence_id,
        created_date AS influence_created_date,
        opportunity_id AS influence_opportunity_id,
        campaign_id AS influence_campaign_id,
        contact_id AS influence_contact_id,
        influence AS influence_amount,
        revenue_share
    FROM base
    WHERE base.is_deleted = 'False'

)

SELECT *
FROM final
