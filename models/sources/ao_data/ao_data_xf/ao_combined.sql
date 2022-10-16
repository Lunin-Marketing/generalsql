{{ config(materialized='table') }}

WITH emails AS (

    SELECT
    -- IDs
        {{ dbt_utils.surrogate_key(['action_time', 'message_id','email'])}} AS touchpoint_id,
        message_id,
        email,
        record_id,
        unique_visitor_id,

    --Message Attributes
        message_title,
        subject_line,
        from_address,

    --Action Attributes
        action,
        action_time,
        action_day,
        clicked_url,
        clickthrough_link_name
    FROM {{ref('ao_emails_xf')}}

), forms AS (

    SELECT
    -- IDs
        {{ dbt_utils.surrogate_key(['action_time', 'form_id','email'])}} AS touchpoint_id,
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
    FROM {{ref('ao_forms_xf')}}

), lps AS (

    SELECT 
    -- IDs
        {{ dbt_utils.surrogate_key(['action_time', 'landing_page_id','email'])}} AS touchpoint_id,
        landing_page_id,
        email,
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
        email_domain
    FROM {{ref('ao_landingpages_xf')}}

), media AS (

    SELECT 
    -- IDs
        {{ dbt_utils.surrogate_key(['action_time', 'media_id','email'])}} AS touchpoint_id,
        media_id,
        email,
        record_id,
        unique_visitor_id,

    -- Media Attributes
        media_name,
        
    -- Action Data
        action,
        action_time,
        action_day,

    -- Other Data
        cookie_id,
        ip_address,
        email_domain
    FROM {{ref('ao_media_xf')}}

), webinars AS (

    SELECT 
    -- IDs
        {{ dbt_utils.surrogate_key(['action_time', 'webinar_id','email'])}} AS touchpoint_id,
        webinar_id,
        email,
        record_id,
        unique_visitor_id,

    -- Webinar Attributes
        webinar_title,
        event_id,

    -- Action Data
        action,
        action_time,
        action_day,

    -- Other Data
        email_domain
    FROM {{ref('ao_webinars_xf')}}

), webpages AS (

    SELECT 
    -- IDs
        {{ dbt_utils.surrogate_key(['action_time', 'page_url','email'])}} AS touchpoint_id,
        email,
        unique_visitor_id,

    -- Webpage Attributes
        page_url,
        referral_url,
        visitor_type,
        url_path,
        
    -- Action Data
        action,
        action_time,
        action_day,

    -- Other Data
        attribution_id,
        cookie_id,
        ip_address,
        email_domain
    FROM {{ref('ao_webpages_xf')}}

), combined_base AS (

    SELECT
        touchpoint_id,
        action,
        action_time,
        action_day,
        message_id AS asset_id,
        email,
        message_title AS asset_title,
        subject_line,
        from_address,
        clicked_url,
        clickthrough_link_name,
        null AS referral_url,
        null AS event_id,
        'email' AS asset_type
    FROM emails
    UNION ALL
    SELECT
        touchpoint_id,
        action,
        action_time,
        action_day,
        form_id AS asset_id,
        email,
        form_title AS asset_title,
        null AS subject_line,
        null AS from_address,
        null AS clicked_url,
        null AS clickthrough_link_name,
        referral_url,
        null AS event_id,
        'form' AS asset_type
    FROM forms
    UNION ALL
    SELECT
        touchpoint_id,
        action,
        action_time,
        action_day,
        landing_page_id AS asset_id,
        email,
        landing_page_title AS asset_title,
        null AS subject_line,
        null AS from_address,
        clicked_url,
        clickthrough_link_name,
        referral_url,
        null AS event_id,
        'landing page' AS asset_type
    FROM lps
    UNION ALL
    SELECT
        touchpoint_id,
        action,
        action_time,
        action_day,
        media_id AS asset_id,
        email,
        media_name AS asset_title,
        null AS subject_line,
        null AS from_address,
        null AS clicked_url,
        null AS clickthrough_link_name,
        null AS referral_url,
        null AS event_id,
        'media' AS asset_type
    FROM media
    UNION ALL
    SELECT
        touchpoint_id,
        action,
        action_time,
        action_day,
        webinar_id AS asset_id,
        email,
        webinar_title AS asset_title,
        null AS subject_line,
        null AS from_address,
        null AS clicked_url,
        null AS clickthrough_link_name,
        null AS referral_url,
        event_id,
        'webinar' AS asset_type
    FROM webinars
    UNION ALL
    SELECT
        touchpoint_id,
        action,
        action_time,
        action_day,
        null AS asset_id,
        email,
        page_url AS asset_title,
        null AS subject_line,
        null AS from_address,
        null AS clicked_url,
        null AS clickthrough_link_name,
        referral_url,
        null AS event_id,
        'webpage' AS asset_type
    FROM webpages
)

SELECT *
FROM combined_base