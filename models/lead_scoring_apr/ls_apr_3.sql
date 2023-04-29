{{ config(materialized='table') }}

WITH email_click_base AS (

    SELECT
        message_recipient_email,
        message_click_date,
        'click' AS event,
        3 AS lead_score
    FROM {{ref('ao_emails_xf')}}
    WHERE is_clicked=TRUE
    AND message_click_date >= CURRENT_DATE-116
    AND message_click_date <= CURRENT_DATE-26

), ao_forms AS (

    SELECT *
    FROM {{ref('ao_forms_xf')}}
    WHERE form_date_time >= CURRENT_DATE-116
    AND form_date_time <= CURRENT_DATE-26

), form_base AS (

    SELECT
        form_response_email,
        form_date_time AS form_date,
        'form_submit' AS event,
        20 AS lead_score
    FROM ao_forms
    WHERE form_response_email IS NOT null

), form_calendly AS (

    SELECT
        form_response_email,
        form_date_time AS form_date,
        'form_calendly' AS event,
        30 AS lead_score
    FROM ao_forms
    WHERE form_response_email IS NOT null
    AND form_title = 'Calendly to AO Integration'

), form_5pts_4 AS (

    SELECT
        form_response_email,
        form_date_time AS form_date,
        'form_infuse_channelmarketing' AS event,
        5 AS lead_score
    FROM ao_forms
    WHERE form_response_email IS NOT null
    AND form_title IN ('Content-Syndication-Infuse-Q3-B2B-Multi-Channel-Marketing')

), form_5pts_1 AS (

    SELECT
        form_response_email,
        form_date_time AS form_date,
        'form_infuse_b2bautomation' AS event,
        5 AS lead_score
    FROM ao_forms
    WHERE form_response_email IS NOT null
    AND form_title IN ('ContentSyndication-Infuse-Q3-TheStateOfB2BMarketingAutomation')

), form_5pts_2 AS (

    SELECT
        form_response_email,
        form_date_time AS form_date,
        'form_infuse_matrends' AS event,
        5 AS lead_score
    FROM ao_forms
    WHERE form_response_email IS NOT null
    AND form_title IN ('ContentSyndication-Infuse-Q3-MarketingAutomationTrends2022')

), form_5pts_3 AS (

    SELECT
        form_response_email,
        form_date_time AS form_date,
        'form_infuse_betterma' AS event,
        5 AS lead_score
    FROM ao_forms
    WHERE form_response_email IS NOT null
    AND form_title IN ('ContentSyndication-Infuse-Q3-HowtoFindaBetterMarketingAutomationSolution')

), web_base AS (

    SELECT
       ao_web_view_xf.*
    FROM {{ref('ao_web_view_xf')}}
    WHERE beacon_datetime >= CURRENT_DATE-116
    AND beacon_datetime <= CURRENT_DATE-26

), web_page_base AS (

    SELECT
        beacon_email,
        beacon_datetime::DATE AS web_visit_date,
        'page_view' AS event,
        1 AS lead_score
    FROM web_base
    WHERE beacon_verb = 'h'
    AND beacon_email IS NOT null

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
    AND webinar_registration_date_time >= CURRENT_DATE-116
    AND webinar_registration_date_time <= CURRENT_DATE-26

), webinar_att AS (

    SELECT
        webinar_email,
        webinar_start_date_time::DATE AS webinar_attended_date,
        'web_att' AS event,
        50 AS lead_score
    FROM webinar_base
    WHERE webinar_verb = 'a'
    AND webinar_start_date_time >= CURRENT_DATE-116
    AND webinar_start_date_time <= CURRENT_DATE-26

), ao_ctp AS (

    SELECT
        EMAIL AS email,
        "When"::Date AS ctp_date,
        "Action" AS ctp_action
    FROM {{ref('ao_ctp_xf')}}
    WHERE "When" >= CURRENT_DATE-116
    AND "When" <= CURRENT_DATE-26

), ctp_reg_event AS (

    SELECT
        email,
        ctp_date,
        'ctp_event_reg' AS event,
        5 AS lead_score
    FROM ao_ctp
    WHERE ctp_action = 'registered for an event'

), ctp_att_event AS (

    SELECT
        email,
        ctp_date,
        'ctp_event_att' AS event,
        20 AS lead_score
    FROM ao_ctp
    WHERE ctp_action = 'attended an event'

), ctp_1on1_event AS (

    SELECT
        email,
        ctp_date,
        'ctp_event_meeting' AS event,
        10 AS lead_score
    FROM ao_ctp
    WHERE ctp_action = 'Event 1 to 1 meeting'

), ctp_hr_event AS (

    SELECT
        email,
        ctp_date,
        'ctp_event_hr' AS event,
        50 AS lead_score
    FROM ao_ctp
    WHERE ctp_action = 'Event Handraiser'

-- ), ctp_web_att AS (

--     SELECT

--         50 AS lead_score
--     FROM 

), ctp_content_download AS (

    SELECT
        email,
        ctp_date,
        'ctp_content_download' AS event,
        20 AS lead_score
    FROM ao_ctp
    WHERE ctp_action = 'Content Syndication - Content Download'

-- ), ctp_web_reg AS (

--     SELECT

--         10 AS lead_score
--     FROM 

-- ), ctp_content_hr AS (

--     SELECT

--         50 AS lead_score
--     FROM 

), demo_score_base AS (

    SELECT DISTINCT
        email,
        is_hand_raiser,
        looking_for_ma,
        medium_lead_creation,
        country, 
        title,
        employee_count, 
        department,
        global_region,
        company,
        industry
    FROM {{ref('person_source_xf')}}


), engagement_score_hr AS (

    SELECT
        email,
        100 AS lead_score
    FROM demo_score_base
    WHERE is_hand_raiser = TRUE        

), engagement_score_intent AS (

    SELECT
        email,
        50 AS lead_score
    FROM demo_score_base 
    WHERE medium_lead_creation = 'Intent partner'

), engagement_score_looking AS (

    SELECT
        email,
        50 AS lead_score
    FROM demo_score_base 
    WHERE looking_for_ma = TRUE
    OR LOWER(looking_for_ma) = 'yes'

), demo_score_country AS (

    SELECT
        email,
        -100 AS lead_score
    FROM demo_score_base 
    WHERE LOWER(country) IN ('china','russia')

), demo_score_dir AS (

    SELECT
        email,
        15 AS lead_score
    FROM demo_score_base 
    WHERE LOWER(title) LIKE '%chief%'
        OR LOWER(title) LIKE '%executive%'
        OR LOWER(title) LIKE '%officer%'
        OR LOWER(title) LIKE '%owner%'
        OR LOWER(title) LIKE '%president%'
        OR LOWER(title) LIKE '%ceo%'
        OR LOWER(title) LIKE '%coo%'
        OR LOWER(title) LIKE '%cfo%'
        OR LOWER(title) LIKE '%cmo%'
        OR LOWER(title) LIKE '%cto%'
        OR LOWER(title) LIKE '%founder%'
        OR LOWER(title) LIKE '%principal%'
        OR LOWER(title) LIKE '%partner%'
        OR LOWER(title) LIKE '%vp%'
        OR LOWER(title) LIKE '%head%'
        OR LOWER(title) LIKE '%director%'
        OR LOWER(title) LIKE '%dir%'

), demo_score_mgr AS (

    SELECT
        email,
        5 AS lead_score
    FROM demo_score_base 
    WHERE LOWER(title) LIKE '%manager%'
        OR LOWER(title) LIKE '%man%'
        OR LOWER(title) LIKE '%mgr%'

), demo_score_title AS (

    SELECT
        email,
        5 AS lead_score
    FROM demo_score_base 
    WHERE title IS NOT null

), demo_score_emp_100 AS (

    SELECT
        email,
        20 AS lead_score
    FROM demo_score_base 
    WHERE employee_count > 99

), demo_score_emp_50 AS (

    SELECT
        email,
        10 AS lead_score
    FROM demo_score_base 
    WHERE employee_count > 49
    AND employee_count < 100

), demo_score_mktg_dept AS (

    SELECT
        email,
        15 AS lead_score
    FROM demo_score_base 
    WHERE LOWER(department) LIKE '%marketing%'
        OR LOWER(department) LIKE '%mktg%'

), demo_score_not_mktg_dept AS (

    SELECT
        email,
        5 AS lead_score
    FROM demo_score_base 
    WHERE LOWER(department) NOT LIKE '%marketing%'
        AND LOWER(department) NOT LIKE '%mktg%'

), demo_score_mktg_title AS (

    SELECT
        email,
        10 AS lead_score
    FROM demo_score_base 
    WHERE LOWER(title) LIKE '%demand%'
        OR LOWER(title) LIKE '%lead%'
        OR LOWER(title) LIKE '%ops%'
        OR LOWER(title) LIKE '%operations%'
        OR LOWER(title) LIKE '%digital%'
        OR LOWER(title) LIKE '%campaign%'
        OR LOWER(title) LIKE '%automation%'

), demo_score_region AS (

    SELECT
        email,
        15 AS lead_score
    FROM demo_score_base 
    WHERE LOWER(global_region) LIKE '%emea%'
        OR LOWER(global_region) LIKE '%aunz%'
        OR LOWER(global_region) LIKE '%na%'

), demo_score_industry_any AS (

    SELECT
        email,
        5 AS lead_score
    FROM demo_score_base 
    WHERE industry IS NOT null 

), demo_score_student AS (

    SELECT
        email,
        -100 AS lead_score
    FROM demo_score_base 
    WHERE LOWER(title) LIKE '%student%'
        OR LOWER(title) LIKE '%freelance%'

), demo_score_test_email AS (

    SELECT
        email,
        -100 AS lead_score
    FROM demo_score_base 
    WHERE email LIKE '%@test.com'

), demo_score_competitor AS (

    SELECT
        email,
        -100 AS lead_score
    FROM demo_score_base 
    WHERE LOWER(company) LIKE '%marketo%'
        OR LOWER(company) LIKE '%pardot%'
        OR LOWER(company) LIKE '%hubspot%'
        OR LOWER(company) LIKE '%eloqua%'

), demo_score_intern AS (

    SELECT
        email,
        -20 AS lead_score
    FROM demo_score_base 
    WHERE LOWER(title) LIKE '%intern%'

), demo_score_emp_10 AS (

    SELECT
        email,
        -40 AS lead_score
    FROM demo_score_base 
    WHERE employee_count >= 0
    AND employee_count <= 10

), demo_score_emp_30 AS (

    SELECT
        email,
        -30 AS lead_score
    FROM demo_score_base 
    WHERE employee_count >= 11
    AND employee_count <= 30

), demo_score_emp_49 AS (

    SELECT
        email,
        -20 AS lead_score
    FROM demo_score_base 
    WHERE employee_count >= 31
    AND employee_count <= 50

), engagement_score_prep AS (

    SELECT
        email,
        IFNULL(lead_score,0) AS lead_score
    FROM engagement_score_hr
    UNION ALL
    SELECT
        email,
        IFNULL(lead_score,0) AS lead_score
    FROM engagement_score_intent
    UNION ALL
    SELECT
        email,
        IFNULL(lead_score,0) AS lead_score
    FROM engagement_score_looking

), engagement_score_final AS (

    SELECT
        LOWER(email) AS email,
        SUM(lead_score) AS lead_score
    FROM engagement_score_prep
    GROUP BY 1

), demo_score_combined AS (
    SELECT
        email,
        IFNULL(lead_score,0) AS lead_score
    FROM demo_score_country
    UNION ALL
    SELECT 
        email,
        IFNULL(lead_score,0) AS lead_score
    FROM demo_score_dir
    UNION ALL
    SELECT
        email,
        IFNULL(lead_score,0) AS lead_score
    FROM demo_score_mgr
    UNION ALL
    SELECT
        email,
        IFNULL(lead_score,0) AS lead_score
    FROM demo_score_title
    UNION ALL
    SELECT
        email,
        IFNULL(lead_score,0) AS lead_score
    FROM demo_score_emp_100
    UNION ALL
    SELECT
        email,
        IFNULL(lead_score,0) AS lead_score
    FROM demo_score_emp_50
    UNION ALL
    SELECT
        email,
        IFNULL(lead_score,0) AS lead_score
    FROM demo_score_mktg_title
    UNION ALL
    SELECT
        email,
        IFNULL(lead_score,0) AS lead_score
    FROM demo_score_region
    UNION ALL
    SELECT
        email,
        IFNULL(lead_score,0) AS lead_score
    FROM demo_score_industry_any
    UNION ALL
    SELECT
        email,
        IFNULL(lead_score,0) AS lead_score
    FROM demo_score_student
    UNION ALL
    SELECT
        email,
        IFNULL(lead_score,0) AS lead_score
    FROM demo_score_test_email
    UNION ALL
    SELECT
        email,
        IFNULL(lead_score,0) AS lead_score
    FROM demo_score_competitor
    UNION ALL
    SELECT
        email,
        IFNULL(lead_score,0) AS lead_score
    FROM demo_score_intern
    UNION ALL
    SELECT
        email,
        IFNULL(lead_score,0) AS lead_score
    FROM demo_score_emp_10
    UNION ALL
    SELECT
        email,
        IFNULL(lead_score,0) AS lead_score
    FROM demo_score_emp_30
    UNION ALL
    SELECT
        email,
        IFNULL(lead_score,0) AS lead_score
    FROM demo_score_emp_49

), demo_score AS (

    SELECT
        LOWER(email) AS email,
        SUM(lead_score) AS lead_score
    FROM demo_score_combined
    GROUP BY 1

), demo_score_final AS (

    SELECT
        email,
        CASE 
            WHEN lead_score > 99 THEN 20
            WHEN lead_score > 49 THEN 10
            WHEN lead_score IS NULL THEN 0
            ELSE 0
        END AS demo_lead_score
    FROM demo_score

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
    UNION ALL
    SELECT DISTINCT
        LOWER(email)
    FROM ctp_reg_event
    UNION ALL
    SELECT DISTINCT
        LOWER(email)
    FROM ctp_att_event
    UNION ALL
    SELECT DISTINCT
        LOWER(email)
    FROM ctp_1on1_event
    UNION ALL
    SELECT DISTINCT
        LOWER(email)
    FROM ctp_hr_event
    UNION ALL
    SELECT DISTINCT
        LOWER(email)
    FROM ctp_content_download

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
    UNION ALL 
    SELECT *
    FROM ctp_reg_event
    UNION ALL
    SELECT *
    FROM ctp_att_event
    UNION ALL
    SELECT *
    FROM ctp_1on1_event
    UNION ALL
    SELECT *
    FROM ctp_hr_event
    UNION ALL
    SELECT *
    FROM ctp_content_download
    UNION ALL
    SELECT *
    FROM form_calendly
    UNION ALL
    SELECT *
    FROM form_5pts_4
    UNION ALL
    SELECT *
    FROM form_5pts_1
    UNION ALL
    SELECT *
    FROM form_5pts_2
    UNION ALL
    SELECT *
    FROM form_5pts_3

), lead_score_prep AS (

    SELECT
        LOWER(message_recipient_email) AS email,
        message_click_date AS score_date,
        event,
        SUM(lead_score) AS lead_score
    FROM lead_score_combined
    GROUP BY 1,2,3

), lead_score_final AS (

    SELECT
        lead_score_prep.email,
        score_date,
        event,
        lead_score_prep.lead_score,
        IFNULL(demo_score_final.demo_lead_score,0) AS demo_lead_score,
        IFNULL(engagement_score_final.lead_score,0) AS engagement_score
    FROM lead_score_prep
    LEFT JOIN demo_score_final
        ON lead_score_prep.email=demo_score_final.email
    LEFT JOIN engagement_score_final
        ON lead_score_prep.email=engagement_score_final.email

), final AS (

    SELECT
        email_final.email,
        lead_score_final.score_date,
        lead_score_final.event,
        lead_score_final.lead_score,
        lead_score_final.demo_lead_score,
        lead_score_final.engagement_score,
        SUM(lead_score_final.lead_score) OVER (PARTITION BY email_final.email ORDER BY lead_score_final.score_date ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) AS total_lead_score
    FROM email_final
    LEFT JOIN lead_score_final
        ON LOWER(email_final.email)=LOWER(lead_score_final.email)
    WHERE email_final.email LIKE '%@%'

)

SELECT *
FROM final
ORDER BY 1,2