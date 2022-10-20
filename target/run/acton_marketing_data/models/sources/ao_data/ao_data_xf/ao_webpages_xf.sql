

  create  table "acton"."dbt_actonmarketing"."ao_webpages_xf__dbt_tmp"
  as (
    

SELECT 
-- IDs
    contact_e_mail AS email,
    unique_visitor_id,

-- Webpage Attributes
    page_url,
    referral_url,
    visitor_type,
    url_path_info AS url_path,
    SPLIT_PART(SPLIT_PART(page_url,'utm_campaign=',2),'&',1) AS campaign,
    SPLIT_PART(SPLIT_PART(page_url,'utm_channel=',2),'&',1) AS channel,
    SPLIT_PART(SPLIT_PART(page_url,'utm_source=',2),'&',1) AS source,
    SPLIT_PART(SPLIT_PART(page_url,'utm_medium=',2),'&',1) AS medium,
    SPLIT_PART(SPLIT_PART(page_url,'utm_content=',2),'&',1) AS content,
    
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
WHERE contact_e_mail IS NOT null
  );