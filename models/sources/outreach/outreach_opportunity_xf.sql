{{ config(materialized='table') }}

WITH base AS (

    SELECT *
    FROM {{source('outreach_sf','opportunity')}}
    
)

SELECT

    --IDs
    prospecting_rep_id AS prospecting_rep_id,
    sharing_team_id AS sharing_team_id,
    relationship_account_id AS or_opp_account_id,
    relationship_creator_id AS or_opp_creator_id,
    id AS or_opp_id,
    relationship_opportunity_stage_id AS or_opp_opportunity_stage_id,
    relationship_owner_id AS or_opp_owner_id,
    relationship_stage_id AS or_opp_stage_id,
    relationship_updater_id AS or_opp_updater_id,

    --Opp Data
    name AS or_opp_name,
    amount AS or_opp_amount,
    description AS or_opp_description,
    map_link AS map_link,
    map_next_steps AS map_next_steps,
    map_status AS map_status,
    next_step AS next_step,
    opportunity_type AS opportunity_type,
    probability AS probability,
    health_score AS health_score,
    health_category AS health_category,
    
    --Dates
    touched_at AS touched_at,
    updated_at AS updated_at,
    close_date AS close_date,
    created_at AS created_at,
    external_created_at AS external_created_at,

    --OR Data
    type AS or_type
FROM base