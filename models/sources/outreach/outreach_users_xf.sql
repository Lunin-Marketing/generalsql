{{ config(materialized='table') }}

WITH base AS (

    SELECT *
    FROM {{source('outreach_sf','users')}}
    WHERE _fivetran_deleted = FALSE

), final AS (

    SELECT DISTINCT
    --IDs
        id AS user_id,
        accounts_view_id,
        opportunities_view_id,
        prospects_view_id,
        relationship_mailbox_id,
        relationship_creator_id,
        relationship_profile_id,
        relationship_role_id,
        user_guid,
        global_id,

    --Dates
        current_sign_in_at AS current_sign_in_date,
        last_sign_in_at AS last_sign_in_date,
        password_expires_at,
        created_at AS created_date,
        updated_At AS updated_date,

    --User Data
        email AS user_email,
        first_name,
        last_name,
        name AS user_full_name,
        primary_timezone AS user_timezone,
        title AS user_title,
        username,
    
    --User Engagement Data
        mailings_delivered_count,
        tasks_due_count,
        mailings_replied_count,
        active_prospects_count
    FROM base

)

SELECT *
FROM final