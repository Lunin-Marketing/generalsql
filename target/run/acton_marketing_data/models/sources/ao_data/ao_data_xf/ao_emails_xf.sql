

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
    SPLIT_PART(SPLIT_PART(clicked_url,'utm_campaign=',2),'&',1) AS campaign,
    SPLIT_PART(SPLIT_PART(clicked_url,'utm_channel=',2),'&',1) AS channel,
    SPLIT_PART(SPLIT_PART(clicked_url,'utm_source=',2),'&',1) AS source,
    SPLIT_PART(SPLIT_PART(clicked_url,'utm_medium=',2),'&',1) AS medium,
    SPLIT_PART(SPLIT_PART(clicked_url,'utm_content=',2),'&',1) AS content,
    clicked_url,
    clickthrough_link_name
FROM "acton"."dbt_actonmarketing"."ao_emails"
WHERE action = 'CLICKED'
AND recipient_e_mail IS NOT null
AND recipient_e_mail NOT LIKE 'unknown%'
  );