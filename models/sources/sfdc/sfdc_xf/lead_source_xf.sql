{{ config(materialized='table') }}

WITH base AS (

SELECT *
FROM {{ source('salesforce', 'lead') }}

), final AS (

    SELECT
        id AS lead_id,
        is_deleted,
        base.created_by_id,
        creator.user_name AS created_by_name,
        first_name,
        last_name, 
        title,
        company,
        state,
        country,
        email,
        lead_source,
        status AS lead_status,
        de_industry_c AS industry,
        CASE
            WHEN de_industry_c IN ('Business Services') THEN 'Business Services'
            WHEN de_industry_c IN ('Finance','Insurance') THEN 'Finance'
            WHEN de_industry_c IN ('Manufacturing') THEN 'Manufacturing'
            WHEN de_industry_c IN ('Software','Telecommunications') THEN 'SoftCom'
            ELSE 'Other'
        END AS industry_bucket,
        annual_revenue,
        number_of_employees,
        owner_id AS lead_owner_id,
        is_converted,
        DATE_TRUNC('day',converted_date)::Date AS converted_date,
        converted_account_id,
        converted_contact_id,
        converted_opportunity_id,
        last_modified_date,
        DATE_TRUNC('day',created_date)::Date AS created_date,
        system_modstamp AS systemmodstamp,
        DATE_TRUNC('day',mql_created_date_c)::Date AS mql_created_date,
        DATE_TRUNC('day',mql_most_recent_date_c)::Date AS mql_most_recent_date,
        DATE_TRUNC('day',date_time_to_working_c)::Date AS working_date,
        account_c AS account_id,
        no_longer_with_company_c AS no_longer_with_company,
        ft_utm_channel_c AS channel_first_touch,
        lt_utm_channel_c AS channel_last_touch,
        lt_utm_medium_c AS medium_last_touch,
        lt_utm_content_c AS content_last_touch,
        lt_utm_source_c AS source_last_touch,
        lt_utm_campaign_c AS campaign_last_touch,
        channel_lead_creation_c AS channel_lead_creation,
        medium_lead_creation_c AS medium_lead_creation,
        hand_raiser_c AS is_hand_raiser,
        ft_subchannel_c AS subchannel_first_touch,
        lt_subchannel_c AS subchannel_last_touch,
        lc_subchannel_c AS subchannel_lead_creation,
        ft_offer_asset_type_c AS offer_asset_type_first_touch,
        ft_offer_asset_subtype_c AS offer_asset_subtype_first_touch,
        ft_offer_asset_topic_c AS offer_asset_topic_first_touch,
        ft_offer_asset_name_c AS offer_asset_name_first_touch,
        lc_offer_asset_type_c AS offer_asset_type_lead_creation,
        lc_offer_asset_subtype_c AS offer_asset_subtype_lead_creation,
        lc_offer_asset_topic_c AS offer_asset_topic_lead_creation,
        lc_offer_asset_name_c AS offer_asset_name_lead_creation,
        lt_offer_asset_type_c AS offer_asset_type_last_touch,
        lt_offer_asset_subtype_c AS offer_asset_subtype_last_touch,
        lt_offer_asset_topic_c AS offer_asset_topic_last_touch,
        lt_offer_asset_name_c AS offer_asset_name_last_touch,
        lean_data_a_2_b_account_c AS lean_data_account_id,
        de_current_marketing_automation_c AS current_ma,
        de_current_crm_c AS current_crm,
        DATE_TRUNC('day',marketing_lead_creation_date_c)::Date AS marketing_created_date,
        mql_created_time_c AS mql_created_datetime,
        mql_most_recent_time_c AS mql_most_recent_datetime,
        article_14_notice_date_c AS article_14_notice_date,
        x_9883_lead_score_c AS lead_score,
        ft_utm_medium_c AS medium_first_touch,
        ft_utm_source_c AS source_first_touch,
        ft_utm_campaign_c AS campaign_first_touch,
        channel_lead_creation_c AS lead_channel_forecast,
        email_bounced_reason,
        email_bounced_reason_c AS email_bounced_reason_new,
        legitimate_basis_c AS legitimate_basis,
        email_bounced_date,
        email_bounced_date_c AS email_bounced_date_new,
        source_lead_creation_c AS source_lead_creation,
        campaign_lead_creation_c AS campaign_lead_creation,
        firmographic_demographic_lead_score_c AS firmographic_demographic_lead_score,
        base.sales_loft_most_recent_cadence_name_c AS most_recent_salesloft_cadence,
        base.looking_for_ma_c AS looking_for_ma,
        do_not_contact_c AS do_not_contact,
        form_consent_opt_in_c AS form_consent_opt_in,
        CASE 
            WHEN annual_revenue <= 49999999 THEN 'SMB'
            WHEN annual_revenue > 49999999 AND annual_revenue <= 499999999 THEN 'Mid-Market'
            WHEN annual_revenue > 499999999 THEN 'Enterprise'
        END AS company_size_rev,
        CASE 
            WHEN LOWER(channel_lead_creation_c) = 'organic' THEN 'Organic'
            WHEN LOWER(channel_lead_creation_c) IS null AND medium_lead_creation_c IS null AND source_lead_creation_c IS null THEN 'Unknown'
            WHEN LOWER(channel_lead_creation_c) = 'product' AND LOWER(medium_lead_creation_c) = 'product - login' THEN 'Product - Lead'
            WHEN LOWER(channel_lead_creation_c) = 'social' AND LOWER(medium_lead_creation_c) = 'social-organic' THEN 'Social - Organic'
            WHEN LOWER(channel_lead_creation_c) = 'social' AND LOWER(medium_lead_creation_c) = 'social_organic' THEN 'Social - Organic'
            WHEN LOWER(channel_lead_creation_c) = 'social' AND LOWER(medium_lead_creation_c) = 'social-paid' THEN 'Social - Paid'
            WHEN LOWER(channel_lead_creation_c) IS null AND LOWER(medium_lead_creation_c) = 'social-paid' THEN 'Social - Paid'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) = 'cpc' THEN 'PPC - Display'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) = 'display' THEN 'PPC - Display'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) = 'intent partner' THEN 'PPC - Intent'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) = 'search' THEN 'PPC - Paid Search'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) = 'social-paid' THEN 'PPC - Social'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) IS NOT null THEN 'PPC - Paid Search'
            WHEN LOWER(channel_lead_creation_c) = 'email' AND LOWER(source_lead_creation_c) like '%act-on%' THEN 'Email - Paid' 
            WHEN LOWER(channel_lead_creation_c) = 'email' AND LOWER(medium_lead_creation_c) = 'email_paid' THEN 'Email - Paid'
            WHEN LOWER(channel_lead_creation_c) = 'email' AND LOWER(medium_lead_creation_c) = 'email-paid' THEN 'Email - Paid'
            WHEN LOWER(channel_lead_creation_c) = 'email' AND LOWER(medium_lead_creation_c) = 'email- paid' THEN 'Email - Paid' 
            WHEN LOWER(channel_lead_creation_c) = 'email' AND LOWER(medium_lead_creation_c) = 'email_inhouse' THEN 'Email - Paid' 
            WHEN LOWER(channel_lead_creation_c) = 'email' AND LOWER(medium_lead_creation_c) = 'paid-email' THEN 'Email - Paid' 
            WHEN LOWER(channel_lead_creation_c) = 'email' AND LOWER(medium_lead_creation_c) = 'email' THEN 'Email - Paid' 
            WHEN LOWER(channel_lead_creation_c) = 'email' AND LOWER(medium_lead_creation_c) = 'syndication_partner' THEN 'Email - Paid' 
            WHEN LOWER(channel_lead_creation_c) = 'ppl' AND LOWER(medium_lead_creation_c) = 'syndication partner' THEN 'PPL - Syndication'  
            WHEN LOWER(channel_lead_creation_c) = 'ppl' AND LOWER(medium_lead_creation_c) = 'intent partner' THEN 'PPL - Intent'
            WHEN LOWER(channel_lead_creation_c) = 'ppl' THEN 'PPL'
            WHEN LOWER(channel_lead_creation_c) = 'prospecting' AND LOWER(medium_lead_creation_c) = 'intent partner' THEN 'Prospecting - Intent Partners'
            WHEN LOWER(channel_lead_creation_c) = 'events' AND LOWER(medium_lead_creation_c) = 'webinar' THEN 'Webinar'
            WHEN LOWER(channel_lead_creation_c) IN ('event','events') THEN 'Events and Trade Shows'
            WHEN LOWER(channel_lead_creation_c) = 'partner' THEN 'Partners'
            WHEN LOWER(medium_lead_creation_c) = 'virtualevent' THEN 'Events and Trade Shows'
            WHEN LOWER(channel_lead_creation_c) = 'prospecting' AND LOWER(medium_lead_creation_c) = 'sdr' THEN 'Prospecting - SDR'
            WHEN LOWER(channel_lead_creation_c) = 'prospecting' AND LOWER(medium_lead_creation_c) = 'rsm' THEN 'Prospecting - RSM'
            WHEN LOWER(channel_lead_creation_c) = 'prospecting' AND LOWER(medium_lead_creation_c) = 'channel management' THEN 'Prospecting - Channel'
            WHEN LOWER(channel_lead_creation_c) = 'prospecting' AND LOWER(medium_lead_creation_c) = 'third-party' THEN 'Prospecting - Third Party'
            WHEN LOWER(channel_lead_creation_c) = 'predates attribution' AND LOWER(medium_lead_creation_c) = 'predates attribution' THEN 'Predates Attribution'
            ELSE 'Other'
        END AS channel_bucket_details,
        CASE 
            WHEN LOWER(channel_lead_creation_c) = 'organic' THEN 'Organic'
            WHEN LOWER(channel_lead_creation_c) IS null AND medium_lead_creation_c IS null AND source_lead_creation_c IS null THEN 'Unknown'
            WHEN LOWER(channel_lead_creation_c) = 'product' AND LOWER(medium_lead_creation_c) = 'product - login' THEN 'Product'
            WHEN LOWER(channel_lead_creation_c) = 'social' AND LOWER(medium_lead_creation_c) = 'social-organic' THEN 'Social'
            WHEN LOWER(channel_lead_creation_c) = 'social' AND LOWER(medium_lead_creation_c) = 'social_organic' THEN 'Social'
            WHEN LOWER(channel_lead_creation_c) = 'social' AND LOWER(medium_lead_creation_c) = 'social-paid' THEN 'Social'
            WHEN LOWER(channel_lead_creation_c) IS null AND LOWER(medium_lead_creation_c) = 'social-paid' THEN 'Social'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) = 'cpc' THEN 'PPC'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) = 'display' THEN 'PPC'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) = 'intent partner' THEN 'PPC'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) = 'search' THEN 'PPC'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) = 'social-paid' THEN 'PPC'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) IS NOT null THEN 'PPC'
            WHEN LOWER(channel_lead_creation_c) = 'email' AND LOWER(source_lead_creation_c) like '%act-on%' THEN 'Email' 
            WHEN LOWER(channel_lead_creation_c) = 'email' AND LOWER(medium_lead_creation_c) = 'email_paid' THEN 'Email'
            WHEN LOWER(channel_lead_creation_c) = 'email' AND LOWER(medium_lead_creation_c) = 'email-paid' THEN 'Email'
            WHEN LOWER(channel_lead_creation_c) = 'email' AND LOWER(medium_lead_creation_c) = 'email- paid' THEN 'Email' 
            WHEN LOWER(channel_lead_creation_c) = 'email' AND LOWER(medium_lead_creation_c) = 'email_inhouse' THEN 'Email' 
            WHEN LOWER(channel_lead_creation_c) = 'email' AND LOWER(medium_lead_creation_c) = 'paid-email' THEN 'Email' 
            WHEN LOWER(channel_lead_creation_c) = 'email' AND LOWER(medium_lead_creation_c) = 'email' THEN 'Email' 
            WHEN LOWER(channel_lead_creation_c) = 'email' AND LOWER(medium_lead_creation_c) = 'syndication_partner' THEN 'Email' 
            WHEN LOWER(channel_lead_creation_c) = 'ppl' AND LOWER(medium_lead_creation_c) = 'syndication partner' THEN 'PPL'  
            WHEN LOWER(channel_lead_creation_c) = 'ppl' AND LOWER(medium_lead_creation_c) = 'intent partner' THEN 'PPL'
            WHEN LOWER(channel_lead_creation_c) = 'ppl' THEN 'PPL'
            WHEN LOWER(channel_lead_creation_c) = 'prospecting' AND LOWER(medium_lead_creation_c) = 'intent partner' THEN 'Prospecting'
            WHEN LOWER(channel_lead_creation_c) = 'events' AND LOWER(medium_lead_creation_c) = 'webinar' THEN 'Events'
            WHEN LOWER(channel_lead_creation_c) IN ('event','events') THEN 'Events'
            WHEN LOWER(channel_lead_creation_c) = 'partner' THEN 'Partners'
            WHEN LOWER(medium_lead_creation_c) = 'virtualevent' THEN 'Events'
            WHEN LOWER(channel_lead_creation_c) = 'prospecting' AND LOWER(medium_lead_creation_c) = 'sdr' THEN 'Prospecting'
            WHEN LOWER(channel_lead_creation_c) = 'prospecting' AND LOWER(medium_lead_creation_c) = 'rsm' THEN 'Prospecting'
            WHEN LOWER(channel_lead_creation_c) = 'prospecting' AND LOWER(medium_lead_creation_c) = 'channel management' THEN 'Prospecting'
            WHEN LOWER(channel_lead_creation_c) = 'prospecting' AND LOWER(medium_lead_creation_c) = 'third-party' THEN 'Prospecting'
            WHEN LOWER(channel_lead_creation_c) = 'predates attribution' AND LOWER(medium_lead_creation_c) = 'predates attribution' THEN 'Predates Attribution'
            ELSE 'Other'
        END AS channel_bucket,
        CASE
            WHEN country IS NOT null AND country IN ('GB','UK','IE','DE','DK','FI','IS','NO','SE','FR','AL','AD','AM','AT','BY','BE','BA','BG','HR','CS','CY','CZ','EE','FX','GE','GR','HU','IT','LV','LI','LT','LU','MK','MT','MD','MC','ME','NL','PL','PT','RO','SM','RS','SJ','SK','SI','ES','CH','UA','VA','FO','GI','GG','IM','JE','XK','RU','United Kingdom','England') THEN 'EUROPE'
            WHEN country IS NOT null AND country IN ('JP','KR','CN','MN','TW','VN','HK','LA','TH','KH','PH','MY','SG','ID','LK','IN','NP','BT','MM','PK','AF','KG','UZ','TM','KZ') THEN 'APJ'
            WHEN country IS NOT null AND country IN ('AU','CX','NZ','NF','Australia','New Zealand') THEN 'AUNZ'
            WHEN country IS NOT null AND country IN ('AR','BO','BR','BZ','CL','CO','CU','CR','DO','EC','FK','GF','GS','GY','GT','HN','MX','NI','PA','PE','PR','PY','SR','SV','UY','VE')THEN 'LATAM'
            WHEN state IS NOT null AND state IN ('CA','NV','UT','AK','MO','CO','HI','OK','IL','AR','NE','MI','KS','OR','WA','ID','WI','MN','ND','SD','MT','WY','IA','NB','ON','PE','QC','AB','BC','MB','SK','NL','NS','YT','NU','NT') THEN 'NA-WEST'
            WHEN state IS NOT null AND state IN ('NY','CT','MA','VT','NH','ME','NJ','RI','TX','AZ','NM','MS','LA','AL','TN','KY','OH','IN','GA','FL','NC','SC','PA','DC','DE','MD','VA','WV') THEN 'NA-EAST'
            WHEN country IS NOT null AND country IN ('AG','AI','AN','AW','BB','BM','BS','DM','GD','GP','HT','JM','KN','LC','MQ','MS','TC','TT','VC','VG','VI') THEN 'NA-EAST'
            WHEN country IS NOT null AND country IN ('US','CA') AND state IS null  THEN 'NA-NO-STATEPROV'
            WHEN country IS NOT null AND state IS NOT null THEN 'ROW'
            ELSE 'Unknown'
        END AS global_region,
        COALESCE(account_c,lean_data_a_2_b_account_c) AS person_account_id
    FROM base
    LEFT JOIN {{ref('user_source_xf')}} creator ON
    base.created_by_id=creator.user_id
    WHERE base.owner_id != '00Ga0000003Nugr' -- AO-Fake Leads
    AND base.is_deleted = 'False'

)

SELECT 
    final.*,
    CASE
        WHEN global_region IN ('EUROPE','ROW','AUNZ') THEN 'EMEA'
        WHEN company_size_rev IN ('SMB') OR company_size_rev IS null THEN 'Velocity'
        WHEN company_size_rev IN ('Mid-Market','Enterprise') THEN 'Upmarket'
        ELSE null
    END AS segment,
    CASE
        WHEN lead_status = 'Current Customer' THEN true
        ELSE False
    END AS is_current_customer
FROM final