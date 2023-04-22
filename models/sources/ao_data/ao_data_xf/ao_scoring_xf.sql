{{ config(materialized='table') }}
--clicked email: 3
--submit form: 20
--web page: 1
--reg web: -10
--att web: 50
--CTP Reg Event: 5
--CTP Att Event: 20
--CTP Event 1:1: 10
--CTP Event HR: 50
--CTP Att web: 50
--CTP Content Synd - DL: 20
--CTP Reg Web: 10
--CTP Content Synd HR: 50
--act-on.com/marketo-vs-act-on/ :20
-- act-on.com/hubspot-vs-act-on/ :20
-- act-on.com/pardot-vs-act-on/ :20
-- act-on.com/eloqua-vs-act-on/ :20
--act-on.com/pricing/ : 20
--act-on.com/learn/case-studies/ : 15
--3 points each
-- act-on.com/product/website-and-landing-page-optimization/
-- act-on.com/product/dynamic-web-forms/
-- act-on.com/product/lead-scoring/
-- act-on.com/product/marketing-segmentation/
-- act-on.com/product/marketing-automation/
-- act-on.com/product/marketing-analytics/
-- act-on.com/product/martech-stack-integrations/
-- act-on.com/product/seo-audit-tools/
-- act-on.com/product/sms-marketing/
-- act-on.com/product/social-media-automation/
-- act-on.com/product/account-based-marketing/
-- act-on.com/product/email-marketing-automation/
-- act-on.com/industries/banking/
-- act-on.com/industries/financial-advisors/
-- act-on.com/industries/insurance/
-- act-on.com/industries/manufacturing/
-- act-on.com/industries/technology/
-- act-on.com/industries/agencies/
-- act-on.com/industries/business/


WITH email_click_base AS (

    SELECT DISTINCT
        message_recipient_email,
        message_click_date,
        3 AS lead_score
    FROM {{ref('ao_emails_xf')}}
    WHERE is_clicked=TRUE

)