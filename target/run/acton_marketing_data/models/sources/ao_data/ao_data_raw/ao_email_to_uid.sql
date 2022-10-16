

  create  table "acton"."dbt_actonmarketing"."ao_email_to_uid__dbt_tmp"
  as (
    

WITH base AS (

    SELECT
        recipient_e_mail AS email,
        unique_visitor_id
    FROM "acton"."dbt_actonmarketing"."ao_emails"
    WHERE recipient_e_mail NOT LIKE 'unknown%'
    AND recipient_e_mail IS NOT null
    UNION ALL
    SELECT
        email,
        unique_visitor_id
    FROM "acton"."dbt_actonmarketing"."ao_forms"
    WHERE email NOT LIKE 'unknown%'
    AND email IS NOT null
    UNION ALL
    SELECT
        e_mail_address AS email,
        unique_visitor_id
    FROM "acton"."dbt_actonmarketing"."ao_landingpages"
    WHERE e_mail_address NOT LIKE 'unknown%'
    AND e_mail_address IS NOT null
    UNION ALL
    SELECT
        e_mail_address AS email,
        unique_visitor_id
    FROM "acton"."dbt_actonmarketing"."ao_media"
    WHERE e_mail_address NOT LIKE 'unknown%'
    AND e_mail_address IS NOT null
    UNION ALL
    SELECT
        contact_e_mail AS email,
        unique_visitor_id
    FROM "acton"."dbt_actonmarketing"."ao_webinars"
    WHERE contact_e_mail NOT LIKE 'unknown%'
    AND contact_e_mail IS NOT null
    UNION ALL
    SELECT
        contact_e_mail AS email,
        unique_visitor_id
    FROM "acton"."dbt_actonmarketing"."ao_webpages"
    WHERE contact_e_mail NOT LIKE 'unknown%'
    AND contact_e_mail IS NOT null

)

    SELECT DISTINCT
        email,
        unique_visitor_id
    FROM base
  );