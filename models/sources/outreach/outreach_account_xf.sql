{{ config(materialized='table') }}

WITH base AS (

    SELECT *
    FROM {{source('outreach_sf','account')}}

)

SELECT
    --IDs
    id AS or_account_id,
    custom_id AS or_account_custom_id,
    sharing_team_id AS or_account_sharing_team_id,
    relationship_creator_id AS or_account_creator_id,
    relationship_owner_id AS or_account_owner_id,
    relationship_updater_id AS or_account_updater_id,

    --Account Info
    name AS or_account_name,
    followers, 
    founded_at,
    industry AS or_account_industry, 
    linked_in_employees AS or_account_linkedin_employees,
    linked_in_url AS or_account_linkedin_url,
    locality AS or_account_locality,
    buyer_intent_score,
    company_type,
    description AS or_account_description,
    domain AS or_account_domain,
    natural_name AS or_account_natural_name,
    number_of_employees AS or_account_employee_count,
    website_url AS or_account_website_url,
    custom_4 AS or_account_global_region,
    custom_1 AS or_account_sdr,
    custom_2 AS or_account_do_not_market,
    custom_5 AS or_account_postal_code,

    --OR Data
    created_at,
    type AS or_type,
    external_source,
    named AS is_named,
    touched_at,
    updated_at
    -- custom_3 AS or_account_number_of_open_opportunities
FROM base


