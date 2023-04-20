{{ config(materialized='table') }}

WITH base AS (

    SELECT *
    FROM {{source('outreach_sf','task')}}

)

SELECT
--IDs
    id AS outreach_task_id,
    relationship_subject_id,
    relationship_creator_id,
    relationship_owner_id,
    relationship_completer_id,
    relationship_account_id, 
    relationship_mailing_id,
    relationship_call_id,
    relationship_opportunity_id,
    relationship_prospect_id,
    relationship_sequence_id,
    relationship_sequence_state_id,
    relationship_sequence_step_id,
    relationship_sequence_template_id,
    relationship_task_priority_id,
    relationship_template_id,

-- Task Data
    type AS outreach_type,
    action AS outreach_task_action,
    note AS outreach_task_note,
    state AS outreach_task_state,
    task_type AS outreach_task_type,
    click_count AS outreach_task_click_count,
    relationship_subject_type,

-- Task Dates
    added_at::Date AS outreach_task_added_date,
    completed_at::Date AS outreach_task_completed_date,
    created_at::Date AS outreach_task_created_date,
    due_at::Date AS outreach_task_due_date,
    scheduled_at::Date AS outreach_task_scheduled_date,
    updated_at::Date AS outreach_task_updated_date
FROM base    