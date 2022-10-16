{{ config(materialized='table') }}

SELECT
-- IDs
    message_id,
    recipient_e_mail AS email,
    record_id,
    unique_visitor_id,

--Message Attributes
    message_title,
    subject AS subject_line,
    "from" AS from_address,

--Action Attributes
    action,
    action_time,
    action_day,
    clicked_url,
    clickthrough_link_name
FROM {{ref('ao_emails')}}
WHERE action != 'SENT'
AND recipient_e_mail IS NOT null
AND recipient_e_mail NOT LIKE 'unknown%'