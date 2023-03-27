{{ config(materialized='table') }}

 WITH base AS (

    SELECT *
    FROM {{source('outreach_sf','mailing')}}
    WHERE _fivetran_deleted = FALSE

 ), final AS (

     SELECT DISTINCT
     --IDs
        id AS mailing_id,
        relationship_mailbox_id,
        relationship_opportunity_id,
        relationship_prospect_id,
        relationship_sequence_id,
        relationship_sequence_state_id,
        relationship_sequence_step_id,
        relationship_task_id,
        relationship_template_id,

    --Mailing Data
        click_count, 
        open_count,
        follow_up_task_type,
        mailbox_address AS recipient_email,
        mailing_type,
        state AS mailing_state,
        subject AS mailing_subject,

    --Dates
        bounced_at::Date AS bounce_date,
        clicked_at::Date AS clicked_date,
        created_at::Date AS created_date,
        delivered_at::Date AS delivered_date,
        follow_up_task_scheduled_at::Date AS follow_up_task_scheduled_date,
        marked_as_spam_at::Date AS marked_as_spam_date,
        opened_at::Date AS opened_date,
        replied_at::Date AS replied_date,
        scheduled_at::Date AS scheduled_date,
        unsubscribed_at::Date AS unsubscribed_date
    FROM base
        
 )

 SELECT *
 FROM final