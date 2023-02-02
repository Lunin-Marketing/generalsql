{{ config(materialized='table') }}

WITH base AS (

    SELECT *
    FROM {{source('outreach_sf','sequence_state')}}
   
)

SELECT
    --IDs
    relationship_sequence_step_id AS or_sequence_step_id,
    relationship_account_id AS or_sequence_account_id,
    relationship_creator_id AS or_sequence_creator_id,
    id AS or_sequence_state_id,
    relationship_sequence_id AS or_sequence_id,
    relationship_mailbox_id AS or_sequence_mailbox_id,
    relationship_opportunity_id AS or_sequence_opportunity_id,
    relationship_prospect_id AS or_sequence_prospect_id,

    --Sequence Data
    sequence_exclusivity,
    bounce_count,
    click_count,
    deliver_count,
    failure_count,
    negative_reply_count,
    neutral_reply_count,
    open_count,
    opt_out_count,
    positive_reply_count,
    reply_count,
    schedule_count,
    auto_resume_ooto_prospects,
    include_unsubscribe_links,
    step_overrides_enabled,

    --State Data
    state AS or_sequence_state,
    error_reason,
    pause_reason,

    --Dates
    created_at,
    updated_at,
    active_at,
    call_completed_at,
    replied_at,
    state_changed_at,

    --OR Data
    type AS or_type
FROM base