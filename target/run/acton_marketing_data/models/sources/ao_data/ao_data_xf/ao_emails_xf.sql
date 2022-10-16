

  create  table "acton"."dbt_actonmarketing"."ao_emails_xf__dbt_tmp"
  as (
    

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
FROM "acton"."dbt_actonmarketing"."ao_emails"
  );