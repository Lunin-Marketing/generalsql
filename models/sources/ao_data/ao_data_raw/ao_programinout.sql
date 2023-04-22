{{ config(materialized='table') }}

SELECT
    "recipientId" AS program_recipient_id,
    "emailKey" AS program_email,
    "recipientName" AS program_recipient_name,
    "groupId" AS program_group_id,
    "stepId" AS program_step_id,
    "stepHistory" AS program_step_history,
    "inoutReason" AS program_in_out_reason,
    "sourceId" AS program_source_id,
    "listId" AS program_list_id,
    "accountId" AS program_account_id,
    "verbKey" AS program_verb,
    "dateTime" AS program_date_time,
    "insertTime" AS program_insert_time,
    "fingerprint" AS program_fingerprint 
FROM "9883Data".FACTS.PROGRAMINOUT_9883