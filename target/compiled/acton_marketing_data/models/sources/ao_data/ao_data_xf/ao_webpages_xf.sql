

SELECT 
-- IDs
    contact_e_mail AS email,
    unique_visitor_id,

-- Webpage Attributes
    page_url,
    referral_url,
    visitor_type,
    url_path_info AS url_path,
    
-- Action Data
    action,
    action_time,
    action_day,

-- Other Data
    attribution_id,
    cookie_id,
    ip_address,
    e_mail_domain AS email_domain
FROM "acton"."dbt_actonmarketing"."ao_webpages"