{{ config(materialized='table') }}

WITH prospect AS (

    SELECT *
    FROM {{ref('outreach_prospect_xf')}}

), sequence AS (

    SELECT *
    FROM {{ref('outreach_sequence_xf')}} 

), sequence_state AS (

    SELECT *
    FROM {{ref('outreach_sequence_state_xf')}} 

)

SELECT
    --IDs
    prospect.or_prospect_id,
    prospect.or_prospect_account_id,
    prospect.or_prospect_creator_id,
    prospect.or_prospect_owner_id,
    prospect.or_prospect_persona_id,
    prospect.or_prospect_stage_id,
    prospect.or_prospect_updater_id,
    prospect.external_id,
    prospect.linked_in_id,
    prospect.updater_id,
    prospect.stack_overflow_id,
    prospect.sharing_team_id,


    --Prospect Data
    prospect.or_prospect_email,
    prospect.or_prospect_first_name,
    prospect.or_prospect_last_name,
    prospect.or_prospect_company,
    prospect.or_prospect_account_name,

    --Sequence Data
    sequence.or_sequence_id,
    sequence.or_sequence_owner_id,
    sequence.or_sequence_tag,
    sequence.or_sequence_name,
    sequence.sequence_type

    --Dates


    --OR Data


FROM prospect
LEFT JOIN sequence_state
    ON prospect.or_prospect_id=sequence_state.or_sequence_prospect_id
LEFT JOIN sequence
    ON sequence_state.or_sequence_id=sequence.or_sequence_id
