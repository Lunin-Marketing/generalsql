

  create  table "acton"."dbt_actonmarketing"."ao_webpages__dbt_tmp"
  as (
    

WITH base AS (

    SELECT *
    FROM "acton"."data_studio_s3"."data_studio_webpages"

)

SELECT
    action,
    action_time,
    action_day,
    cookie_id,
    attribution_id,
    contact_e_mail,
    ip_address,
    page_url,
    referral_url,
    visitor_type,
    e_mail_domain,
    unique_visitor_id,
    url_path_info
FROM base
  );