

  create  table "acton"."dbt_actonmarketing"."email_clicks_ao_xf__dbt_tmp"
  as (
    

WITH base AS (

SELECT * 
FROM "acton".public.email_clicks_ao_2021
union all
SELECT * 
FROM "acton".public.email_clicks_ao_20220316

)

SELECT
    "Recipient E-mail" AS email,
    "Action" AS action,
    "Action Day" AS action_day,
    "Action Time" AS action_time,
    "Message ID" AS message_id,
    "subject" AS subject,
    "Message Title" AS message_title, 
    "Automated Program Name" AS automated_program_name,
    "Campaign Name" AS campaign_name,
    "Clicked URL" AS clicked_url
FROM base
  );