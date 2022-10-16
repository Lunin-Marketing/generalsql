{{ config(materialized='table') }}

SELECT 
-- IDs
    landing_page_id,
    e_mail_address AS email,
    record_id,
    unique_visitor_id,

-- LP Attributes
    landing_page_title,
    clicked_url,
    clickthrough_link_name,
    referral_url,

-- Action Data
    action,
    action_time,
    action_day,

-- Other Data
    cookie_id,
    ip_address,
    e_mail_domain AS email_domain
FROM {{ref('ao_landingpages')}}