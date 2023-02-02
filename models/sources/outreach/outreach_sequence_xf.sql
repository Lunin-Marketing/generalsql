{{ config(materialized='table') }}

WITH base AS (

    SELECT *
    FROM {{source('outreach_sf','sequence')}}
  
), sequence_tag AS (

    SELECT *
    FROM {{source('outreach_sf','sequence_tag')}}
)

SELECT

    --IDs
    relationship_creator_id AS or_sequence_creator_id,
    base.id AS or_sequence_id,
    relationship_owner_id AS or_sequence_owner_id,
    relationship_ruleset_id AS or_sequence_ruleset_id,
    relationship_updater_id AS or_sequence_updater_id,


    --Sequence Data
    tag_name AS or_sequence_tag,
    base.name AS or_sequence_name,
    primary_reply_action AS primary_reply_action,
    schedule_interval_type AS schedule_interval_type,
    secondary_reply_action AS secondary_reply_action,
    sequence_type AS sequence_type,
    share_type AS share_type,
    bounce_count AS bounce_count,
    click_count AS click_count,
    deliver_count AS deliver_count,
    duration_in_days AS duration_in_days,
    failure_count AS failure_count,
    max_activations AS max_activations,
    negative_reply_count AS negative_reply_count,
    neutral_reply_count AS neutral_reply_count,
    num_contacted_prospects AS num_contacted_prospects,
    num_replied_prospects AS num_replied_prospects,
    open_count AS open_count,
    opt_out_count AS opt_out_count,
    positive_reply_count AS positive_reply_count,
    primary_reply_pause_duration AS primary_reply_pause_duration,
    reply_count AS reply_count,
    schedule_count AS schedule_count,
    secondary_reply_pause_duration AS secondary_reply_pause_duration,
    sequence_step_count AS sequence_step_count,
    throttle_capacity AS throttle_capacity,
    throttle_max_adds_per_day AS throttle_max_adds_per_day,
    finish_on_reply AS finish_on_reply,
    throttle_paused AS throttle_paused,
    automation_percentage AS automation_percentage,

    --Opp Flags
    enabled AS is_enabled,
    locked AS is_locked,
    transactional AS is_transactional,


    --Dates
    base.created_at AS created_at,
    base.updated_at AS updated_at,
    base.enabled_at AS enabled_at,
    base.last_used_at AS last_used_at,
    throttle_paused_at AS throttle_paused_at,


    --OR Data
    base.description AS or_description,
    base.type AS or_type
FROM base    
LEFT JOIN sequence_tag
    ON base.id=sequence_tag.sequence_id