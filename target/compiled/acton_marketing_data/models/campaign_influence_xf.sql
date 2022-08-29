

WITH base AS (

SELECT *
FROM "acton".salesforce."campaign_influence"

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

)

SELECT *
FROM final