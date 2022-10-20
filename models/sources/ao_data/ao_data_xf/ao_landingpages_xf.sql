{{ config(materialized='table') }}

SELECT 
-- IDs
    landing_page_id,
    e_mail_address AS email,
    record_id,
    unique_visitor_id,

-- LP Attributes
    landing_page_title,
    SPLIT_PART(SPLIT_PART(clicked_url,'utm_campaign=',2),'&',1) AS campaign,
    SPLIT_PART(SPLIT_PART(clicked_url,'utm_channel=',2),'&',1) AS channel,
    SPLIT_PART(SPLIT_PART(clicked_url,'utm_source=',2),'&',1) AS source,
    SPLIT_PART(SPLIT_PART(clicked_url,'utm_medium=',2),'&',1) AS medium,
    SPLIT_PART(SPLIT_PART(clicked_url,'utm_content=',2),'&',1) AS content,
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
WHERE e_mail_address IS NOT null
AND e_mail_address NOT LIKE 'unknown%'