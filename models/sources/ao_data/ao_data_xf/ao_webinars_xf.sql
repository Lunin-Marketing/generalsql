-- {{ config(materialized='table') }}

-- SELECT 
-- -- IDs
--     webinar_id,
--     contact_e_mail AS email,
--     record_id,
--     unique_visitor_id,

-- -- Webinar Attributes
--     webinar_title,
--     event_program_id AS event_id,

-- -- Action Data
--     action,
--     action_time,
--     action_day,

-- -- Other Data
--     e_mail_domain AS email_domain
-- FROM {{ref('ao_webinars')}}
-- WHERE contact_e_mail IS NOT null
-- AND contact_e_mail NOT LIKE 'unknown%'