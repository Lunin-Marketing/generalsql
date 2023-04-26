{{ config(materialized='table') }}

WITH email_click_base AS (

    SELECT
        message_recipient_email,
        message_click_date,
        'click' AS event,
        3 AS lead_score
    FROM {{ref('ao_emails_xf')}}
    WHERE is_clicked=TRUE
    AND message_click_date >= CURRENT_DATE-90

), form_base AS (

    SELECT
        form_response_email,
        form_date_time AS form_date,
        'form_submit' AS event,
        -- form_title,
        20 AS lead_score
    FROM {{ref('ao_forms_xf')}}
    WHERE form_response_email IS NOT null
    AND form_date_time >= CURRENT_DATE-90

), web_base AS (

    SELECT
       ao_web_view_xf.*
    FROM {{ref('ao_web_view_xf')}}

), web_page_base AS (

    SELECT
        beacon_email,
        beacon_datetime::DATE AS web_visit_date,
        'page_view' AS event,
        1 AS lead_score
    FROM web_base
    WHERE beacon_verb = 'h'
    AND beacon_email IS NOT null
    AND beacon_datetime >= CURRENT_DATE-90

), marketo_page_view AS (

    SELECT DISTINCT
        beacon_email,
        beacon_datetime::DATE AS web_visit_date,
        'marketo_page' AS event,
        20 AS lead_score
    FROM web_base
    WHERE beacon_verb = 'h'
    AND beacon_email IS NOT null
    AND beacon_page LIKE '%act-on.com/marketo-vs-act-on%'
    AND beacon_datetime >= CURRENT_DATE-90

), hubspot_page_view AS (

    SELECT DISTINCT
        beacon_email,
        beacon_datetime::DATE AS web_visit_date,
        'hubspot_page' AS event,
        20 AS lead_score
    FROM web_base
    WHERE beacon_verb = 'h'
    AND beacon_email IS NOT null
    AND beacon_page LIKE '%act-on.com/hubspot-vs-act-on/%'
    AND beacon_datetime >= CURRENT_DATE-90

), pardot_page_view AS (

    SELECT DISTINCT
        beacon_email,
        beacon_datetime::DATE AS web_visit_date,
        'pardot_page' AS event,
        20 AS lead_score
    FROM web_base
    WHERE beacon_verb = 'h'
    AND beacon_email IS NOT null
    AND beacon_page LIKE '%act-on.com/pardot-vs-act-on/%'
    AND beacon_datetime >= CURRENT_DATE-90

), eloqua_page_view AS (

    SELECT DISTINCT
        beacon_email,
        beacon_datetime::DATE AS web_visit_date,
        'eloqua_page' AS event,
        20 AS lead_score
    FROM web_base
    WHERE beacon_verb = 'h'
    AND beacon_email IS NOT null
    AND beacon_page LIKE '%act-on.com/eloqua-vs-act-on/%'
    AND beacon_datetime >= CURRENT_DATE-90

), pricing_page_view AS (

    SELECT DISTINCT
        beacon_email,
        beacon_datetime::DATE AS web_visit_date,
        'pricing_page' AS event,
        20 AS lead_score
    FROM web_base
    WHERE beacon_verb = 'h'
    AND beacon_email IS NOT null
    AND beacon_page LIKE '%act-on.com/pricing/%'
    AND beacon_datetime >= CURRENT_DATE-90

), case_study_page_view AS (

    SELECT DISTINCT
        beacon_email,
        beacon_datetime::DATE AS web_visit_date,
        'case_study' AS event,
        15 AS lead_score
    FROM web_base
    WHERE beacon_verb = 'h'
    AND beacon_email IS NOT null
    AND beacon_page LIKE '%act-on.com/learn/case-studies/%'
    AND beacon_datetime >= CURRENT_DATE-90

), three_point_page_view AS (

    SELECT DISTINCT
        beacon_email,
        beacon_datetime::DATE AS web_visit_date,
        'three_pointer' AS event,
        3 AS lead_score
    FROM web_base
    WHERE beacon_verb = 'h'
    AND beacon_email IS NOT null
    AND (beacon_page LIKE '%act-on.com/learn/case-studies/%'
        OR beacon_page LIKE '%act-on.com/product/website-and-landing-page-optimization/%'
        OR beacon_page LIKE '%act-on.com/product/dynamic-web-forms/%'
        OR beacon_page LIKE '%act-on.com/product/lead-scoring/%'
        OR beacon_page LIKE '%act-on.com/product/marketing-segmentation/%'
        OR beacon_page LIKE '%act-on.com/product/marketing-automation/%'
        OR beacon_page LIKE '%act-on.com/product/marketing-analytics/%'
        OR beacon_page LIKE '%act-on.com/product/martech-stack-integrations/%'
        OR beacon_page LIKE '%act-on.com/product/seo-audit-tools/%'
        OR beacon_page LIKE '%act-on.com/product/sms-marketing/%'
        OR beacon_page LIKE '%act-on.com/product/social-media-automation/%'
        OR beacon_page LIKE '%act-on.com/product/account-based-marketing/%'
        OR beacon_page LIKE '%act-on.com/product/email-marketing-automation/%'
        OR beacon_page LIKE '%act-on.com/industries/banking/%'
        OR beacon_page LIKE '%act-on.com/industries/financial-advisors/%'
        OR beacon_page LIKE '%act-on.com/industries/insurance/%'
        OR beacon_page LIKE '%act-on.com/industries/manufacturing/%'
        OR beacon_page LIKE '%act-on.com/industries/technology/%'
        OR beacon_page LIKE '%act-on.com/industries/agencies/%'
        OR beacon_page LIKE '%act-on.com/industries/business/%')
    AND beacon_datetime >= CURRENT_DATE-90

), webinar_base AS (

    SELECT *
    FROM {{ref('ao_bogus_meeting')}}

), webinar_reg AS (

    SELECT
        webinar_email,
        webinar_registration_date_time::DATE AS webinar_registration_date,
        'web_reg' AS event,
        -10 AS lead_score
    FROM webinar_base
    WHERE webinar_verb = 'r'
    AND webinar_registration_date_time >= CURRENT_DATE-90

), webinar_att AS (

    SELECT
        webinar_email,
        webinar_start_date_time::DATE AS webinar_attended_date,
        'web_att' AS event,
        50 AS lead_score
    FROM webinar_base
    WHERE webinar_verb = 'a'
    AND webinar_start_date_time >= CURRENT_DATE-90

-- ), ctp_reg_event AS (

--     SELECT DISTINCT

--         5 AS lead_score
--     FROM 

-- ), ctp_att_event AS (

--     SELECT DISTINCT

--         20 AS lead_score
--     FROM 

-- ), ctp_1on1_event AS (

--     SELECT DISTINCT

--         10 AS lead_score
--     FROM 

-- ), ctp_hr_event AS (

--     SELECT DISTINCT

--         50 AS lead_score
--     FROM 

-- ), ctp_web_att AS (

--     SELECT DISTINCT

--         50 AS lead_score
--     FROM 

-- ), ctp_content_download AS (

--     SELECT DISTINCT

--         20 AS lead_score
--     FROM 

-- ), ctp_web_reg AS (

--     SELECT DISTINCT

--         10 AS lead_score
--     FROM 

-- ), ctp_content_hr AS (

--     SELECT DISTINCT

--         50 AS lead_score
--     FROM 

), email_base AS (

    SELECT DISTINCT
        LOWER(message_recipient_email) AS email
    FROM email_click_base
    UNION ALL
    SELECT DISTINCT
        LOWER(form_response_email)
    FROM form_base
    UNION ALL
    SELECT DISTINCT
        LOWER(beacon_email)
    FROM web_page_base
    UNION ALL
    SELECT DISTINCT
        LOWER(webinar_email)
    FROM webinar_reg

), email_final AS (

    SELECT DISTINCT
        email
    FROM email_base
  
), lead_score_combined AS (

    SELECT *
    FROM email_click_base
    UNION ALL
    SELECT *
    FROM form_base
    UNION ALL
    SELECT *
    FROM web_page_base
    UNION ALL
    SELECT *
    FROM marketo_page_view
    UNION ALL
    SELECT *
    FROM hubspot_page_view
    UNION ALL
    SELECT *
    FROM pardot_page_view
    UNION ALL
    SELECT *
    FROM eloqua_page_view
    UNION ALL
    SELECT *
    FROM pricing_page_view
    UNION ALL
    SELECT *
    FROM case_study_page_view
    UNION ALL
    SELECT *
    FROM three_point_page_view
    UNION ALL
    SELECT *
    FROM webinar_reg
    UNION ALL
    SELECT *
    FROM webinar_att

), lead_score_final AS (

    SELECT
        LOWER(message_recipient_email) AS email,
        message_click_date AS score_date,
        event,
        SUM(lead_score) AS lead_score
    FROM lead_score_combined
    GROUP BY 1,2,3

), final AS (

    SELECT
        email_final.email,
        lead_score_final.score_date,
        lead_score_final.lead_score,
        lead_score_final.event,
        SUM(lead_score_final.lead_score) OVER (PARTITION BY email_final.email ORDER BY lead_score_final.score_date ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) AS total_lead_score
    FROM email_final
    LEFT JOIN lead_score_final
        ON LOWER(email_final.email)=LOWER(lead_score_final.email)
    WHERE email_final.email LIKE '%@%'

)

SELECT *
FROM final
-- WHERE email = 'a-szucs@ti.com'
-- WHERE lead_score != total_lead_score
ORDER BY 1,2