

SELECT 
-- IDs
    document_id AS media_id,
    e_mail_address AS email,
    record_id,
    unique_visitor_id,

-- Media Attributes
    document_name AS media_name,
    
-- Action Data
    action,
    action_time,
    action_day,

-- Other Data
    cookie_id,
    ip_address,
    e_mail_domain AS email_domain
FROM "acton"."dbt_actonmarketing"."ao_media"