

WITH emails AS (

    SELECT
    -- IDs
        md5(cast(coalesce(cast(action_time as TEXT), '') || '-' || coalesce(cast(message_id as TEXT), '') || '-' || coalesce(cast(email as TEXT), '') as TEXT)) AS touchpoint_id,
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
        clickthrough_link_name,
    
    --Attribution Attributes
        campaign,
        channel,
        content,
        medium,
        source
    FROM "acton"."dbt_actonmarketing"."ao_emails_xf"

), forms AS (

    SELECT
    -- IDs
        md5(cast(coalesce(cast(action_time as TEXT), '') || '-' || coalesce(cast(form_id as TEXT), '') || '-' || coalesce(cast(email as TEXT), '') as TEXT)) AS touchpoint_id,
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

    --Attribution Attributes
        -- campaign,
        -- channel,
        -- content,
        -- medium,
        -- source

    --Other Data
        ip_address,
        cookie_id
    FROM "acton"."dbt_actonmarketing"."ao_forms_xf"

), lps AS (

    SELECT 
    -- IDs
        md5(cast(coalesce(cast(action_time as TEXT), '') || '-' || coalesce(cast(landing_page_id as TEXT), '') || '-' || coalesce(cast(email as TEXT), '') as TEXT)) AS touchpoint_id,
        landing_page_id,
        email,
        record_id,
        unique_visitor_id,

    -- LP Attributes
        landing_page_title,
        clicked_url,
        clickthrough_link_name,
        referral_url,

        --Attribution Attributes
        campaign,
        channel,
        content,
        medium,
        source,

    -- Action Data
        action,
        action_time,
        action_day,

    -- Other Data
        cookie_id,
        ip_address,
        email_domain
    FROM "acton"."dbt_actonmarketing"."ao_landingpages_xf"

), media AS (

    SELECT 
    -- IDs
        md5(cast(coalesce(cast(action_time as TEXT), '') || '-' || coalesce(cast(media_id as TEXT), '') || '-' || coalesce(cast(email as TEXT), '') as TEXT)) AS touchpoint_id,
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
    FROM "acton"."dbt_actonmarketing"."ao_media_xf"

), tasks AS (

    SELECT
    --IDs
        md5(cast(coalesce(cast(activity_date as TEXT), '') || '-' || coalesce(cast(task_id as TEXT), '') || '-' || coalesce(cast(email as TEXT), '') as TEXT)) AS touchpoint_id,
        task_id,
        person_id,
        owner_id,
        account_id,
        email,

    --Task Data
        task_subject,
        activity_date,
        task_status,
        task_type,
        is_closed,
        task_created_date,
        close_date
    FROM "acton"."dbt_actonmarketing"."task_source_xf"

), webinars AS (

    SELECT 
    -- IDs
        md5(cast(coalesce(cast(action_time as TEXT), '') || '-' || coalesce(cast(webinar_id as TEXT), '') || '-' || coalesce(cast(email as TEXT), '') as TEXT)) AS touchpoint_id,
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
    FROM "acton"."dbt_actonmarketing"."ao_webinars_xf"

), webpages AS (

    SELECT 
    -- IDs
        md5(cast(coalesce(cast(action_time as TEXT), '') || '-' || coalesce(cast(page_url as TEXT), '') || '-' || coalesce(cast(email as TEXT), '') as TEXT)) AS touchpoint_id,
        email,
        unique_visitor_id,

    -- Webpage Attributes
        page_url,
        referral_url,
        visitor_type,
        url_path,

    --Attribution Attributes
        campaign,
        channel,
        content,
        medium,
        source,
        
    -- Action Data
        action,
        action_time,
        action_day,

    -- Other Data
        attribution_id,
        cookie_id,
        ip_address,
        email_domain
    FROM "acton"."dbt_actonmarketing"."ao_webpages_xf"

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
        campaign,
        channel,
        content,
        medium,
        source,
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
        null AS campaign,
        null AS channel,
        null AS content,
        null AS medium,
        null AS source,
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
        campaign,
        channel,
        content,
        medium,
        source,
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
        null AS campaign,
        null AS channel,
        null AS content,
        null AS medium,
        null AS source,
        null AS clickthrough_link_name,
        null AS referral_url,
        null AS event_id,
        'media' AS asset_type
    FROM media
    UNION ALL
    SELECT
        touchpoint_id,
        task_type AS action,
        activity_date::timestamp AS action_time,
        activity_date AS action_day,
        task_id AS asset_id,
        email,
        task_subject AS asset_title,
        null AS subject_line,
        null AS from_address,
        null AS clicked_url,
        null AS campaign,
        null AS channel,
        null AS content,
        null AS medium,
        null AS source,
        null AS clickthrough_link_name,
        null AS referral_url,
        null AS event_id,
        'task' AS asset_type   
    FROM tasks
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
        null AS campaign,
        null AS channel,
        null AS content,
        null AS medium,
        null AS source,
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
        campaign,
        channel,
        content,
        medium,
        source,
        null AS clickthrough_link_name,
        referral_url,
        null AS event_id,
        'webpage' AS asset_type
    FROM webpages
)

SELECT *
FROM combined_base
WHERE action_day IS NOT null
AND action_day < '2022-10-01'