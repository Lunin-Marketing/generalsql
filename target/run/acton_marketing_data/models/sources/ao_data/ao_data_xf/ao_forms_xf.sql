

  create  table "acton"."dbt_actonmarketing"."ao_forms_xf__dbt_tmp"
  as (
    

SELECT
-- IDs
    form_id,
    email,
    unique_visitor_id,
    record_id,

--Form Attributes
    form_title,

--Action Attributes
    action,
    action_time,
    action_day,
    referral_url,

--Other Data
    ip_address,
    cookie_id
FROM "acton"."dbt_actonmarketing"."ao_forms"
WHERE email IS NOT null
AND email NOT LIKE 'unknown%'
  );