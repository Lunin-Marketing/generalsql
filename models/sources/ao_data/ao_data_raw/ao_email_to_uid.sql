{{ config(materialized='table') }}

WITH base AS (

    SELECT
        recipient_e_mail AS email,
        unique_visitor_id
    FROM {{ref('ao_emails')}}
    WHERE recipient_e_mail NOT LIKE 'unknown%'
    AND recipient_e_mail IS NOT null
    UNION ALL
    SELECT
        email,
        unique_visitor_id
    FROM {{ref('ao_forms')}}
    WHERE email NOT LIKE 'unknown%'
    AND email IS NOT null
    UNION ALL
    SELECT
        e_mail_address AS email,
        unique_visitor_id
    FROM {{ref('ao_landingpages')}}
    WHERE e_mail_address NOT LIKE 'unknown%'
    AND e_mail_address IS NOT null
    UNION ALL
    SELECT
        e_mail_address AS email,
        unique_visitor_id
    FROM {{ref('ao_media')}}
    WHERE e_mail_address NOT LIKE 'unknown%'
    AND e_mail_address IS NOT null
    UNION ALL
    SELECT
        contact_e_mail AS email,
        unique_visitor_id
    FROM {{ref('ao_webinars')}}
    WHERE contact_e_mail NOT LIKE 'unknown%'
    AND contact_e_mail IS NOT null
    UNION ALL
    SELECT
        contact_e_mail AS email,
        unique_visitor_id
    FROM {{ref('ao_webpages')}}
    WHERE contact_e_mail NOT LIKE 'unknown%'
    AND contact_e_mail IS NOT null

)

    SELECT DISTINCT
        email,
        unique_visitor_id
    FROM base