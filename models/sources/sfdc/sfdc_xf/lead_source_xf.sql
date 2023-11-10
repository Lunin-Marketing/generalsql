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
        cfcr_status_reason_c AS status_reason,
        de_department_c AS department,
        de_industry_c AS industry,
        CASE
            WHEN de_industry_c IN ('Advertising & Marketing') THEN 'Advertising & Marketing'
            WHEN de_industry_c IN ('Agriculture','Animals & Livestock','Crops','Forestry') THEN 'Agriculture'
            WHEN de_industry_c IN ('Business Services','Call Centers & Business Centers','Chambers of Commerce','Commercial Printing','Debt Collection','Facilities Management & Commercial Cleaning','Food Service','HR & Staffing','Human Resources & Staffing','Information & Document Management','Management Consulting','Multimedia & Graphic Design','Research & Development','Security Products & Services','Translation & Linguistic Services') THEN 'Business Services'
            WHEN de_industry_c IN ('Architecture, Engineering & Design','Civil Engineering Construction','Commercial & Residential Construction','Construction') THEN 'Construction'
            WHEN de_industry_c IN ('Automotive Service & Collision Repair','Barber Shops & Beauty Salons','Childcare','Cleaning Services','Consumer Services','Funeral Homes & Funeral Related Services','Photography Studio','Repair Services','Weight & Health Management') THEN 'Consumer Services'
            WHEN de_industry_c IN ('Colleges & Universities','Education','K-12 Schools','Training') THEN 'Education'
            WHEN de_industry_c IN ('Electricity, Oil & Gas','Energy, Utilities & Waste','Energy, Utilities & Waste Treatment','Oil & Gas Exploration & Services','Waste Treatment, Environmental Services & Recycling','Water & Water Treatment','Water Treatment','Energy/Utilities') THEN 'Energy/Utilities'
            WHEN de_industry_c IN ('Finance','Insurance','Banking','Brokerage','Credit Cards & Transaction Processing','Investment Banking','Lending & Brokerage','Venture Capital & Private Equity') THEN 'Finance'
            WHEN de_industry_c IN ('Federal','Government','State','Tribal Nations') THEN 'Government'
            WHEN de_industry_c IN ('Ambulance Services','Blood & Organ Banks','Elderly Care Services','Healthcare Services','Medical Laboratories & Imaging Centers','Mental Health & Rehabilitation Facilities','Veterinary Services') THEN 'Healthcare Services'
            WHEN de_industry_c IN ('Holding Companies & Conglomerates') THEN 'Holding Companies'
            WHEN de_industry_c IN ('Amusement Parks, Arcades & Attractions','Cultural & Informational Centers','Fitness & Dance Facilities','Gambling & Gaming','Hospitality','Libraries','Lodging & Resorts','Movie Theaters','Museums & Art Galleries','Performing Arts Theaters','Restaurants','Sports Teams & Leagues','Travel Agencies & Services','Zoos & National Parks') THEN 'Hospitality'
            WHEN de_industry_c IN ('Dental Offices','Hospitals & Physicians Clinics','Medical & Surgical Hospitals','Medical Specialists','Physicians Clinics') THEN 'Hospitals/Clinics'
            WHEN de_industry_c IN ('Manufacturing','Aerospace & Defense','Appliances','Automotive Parts','Boats & Submarines','Building Materials','Chemicals & Related Products','Chemicals, Petrochemicals, Glass & Gases','Cleaning Products','Computer Equipment & Peripherals','Cosmetics, Beauty Supply & Personal Care Products','Electronics','Food & Beverage','Food, Beverages & Tobacco','Furniture','Glass & Clay','Hand, Power & Lawn-care Tools','Health & Nutrition Products','Household Goods','Industrial Machinery & Equipment','Medical Devices & Equipment','Motor Vehicle Parts','Motor Vehicles','Pet Products','Pharmaceuticals','Photographic & Optical Equipment','Plastic, Packaging & Containers','Pulp & Paper','Sporting Goods','Telecommunication Equipment','Test & Measurement Equipment','Textiles & Apparel','Tires & Rubber','Toys & Games','Watches & Jewelry','Wire & Cable') THEN 'Manufacturing'
            WHEN de_industry_c IN ('Broadcasting','Data Collection & Internet Portals','Media & Internet','Music & Music Related Services','Music Production & Services','Newspapers & News Services','Publishing','Social Networks','Ticket Sales') THEN 'Media & Internet'
            WHEN de_industry_c IN ('Metals & Mining') THEN 'Metals & Mining'
            WHEN de_industry_c IN ('Minerals & Mining') THEN 'Minerals & Mining'
            WHEN de_industry_c IN ('Membership Organizations','Non-Profit & Charitable Organizations','Organizations','Religious Organizations') THEN 'Organizations'
            WHEN de_industry_c IN ('Accounting & Accounting Services','Accounting Services','Law Firms & Legal Services') THEN 'Professional Services'
            WHEN de_industry_c IN ('Real Estate') THEN 'Real Estate'
            WHEN de_industry_c IN ('Apparel & Accessories Retail','Auctions','Automobile Dealers','Automobile Parts Stores','Car & Truck Rental','Consumer Electronics & Computers Retail','Convenience Stores, Gas Stations & Liquor Stores','Department Stores, Shopping Centers & Superstores','Drug Stores & Pharmacies','Flowers, Gifts & Specialty Stores','Grocery Retail','Home Improvement & Hardware Retail','Jewelry & Watch Retail','Office Products Retail & Distribution','Other Rental Stores (Furniture, A/V, Construction & Industrial Equipment)','Record, Video & Book Stores','Retail','Sporting & Recreational Equipment Retail','Vitamins, Supplements & Health Stores') THEN 'Retail'
            WHEN de_industry_c IN ('Business Intelligence (BI) Software','Content & Collaboration Software','Custom Software & IT Services','Customer Relationship Management (CRM) Software','Database & File Management Software','Engineering Software','Enterprise Resource Planning (ERP) Software','Financial Software','Healthcare Software','Human Resources Software','Legal Software','Mobile App Development','Multimedia, Games & Graphics Software','Networking Software','Security Software','Storage & System Management Software','Supply Chain Management (SCM) Software','Software') THEN 'Software'
            WHEN de_industry_c IN ('Telecommunications','Cable & Satellite','Internet Service Providers, Website Hosting & Internet-related Services','Telephony & Wireless') THEN 'Telecommunications'
            WHEN de_industry_c IN ('Airlines, Airports & Air Services','Freight & Logistics Services','Rail, Bus & Taxi','Trucking, Moving & Storage','Transportation','Marine Shipping & Transportation') THEN 'Transportation'
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
        DATE_TRUNC('day',base.hand_raiser_date_time_c)::Date AS hand_raiser_date,
        DATE_TRUNC('day',base.last_activity_date)::Date AS last_activity_date,
        account_c AS account_id,
        no_longer_with_company_c AS no_longer_with_company,
        ft_utm_channel_c AS channel_first_touch,
        lt_utm_channel_c AS channel_last_touch,
        lt_utm_medium_c AS medium_last_touch,
        lt_utm_content_c AS content_last_touch,
        content_lead_creation_c AS content_lead_creation,
        term_lead_creation_c AS term_lead_creation,
        lt_utm_source_c AS source_last_touch,
        lt_utm_campaign_c AS campaign_last_touch,
        CASE 
            WHEN LOWER (channel_lead_creation_c) = 'predates attribution'
            THEN lt_utm_channel_c
        END AS channel_lead_creation, 
        CASE 
            WHEN LOWER (medium_lead_creation_c) = 'predates attribution'
            THEN lt_utm_medium_c
        END AS medium_lead_creation, 
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
        x_9883_lead_score_c AS lead_score_9883,
        CASE
            WHEN data_enrich_employee_count_c IS NULL THEN 20
            ELSE 0
        END AS de_ec_subtraction,
        CASE
            WHEN firmographic_demographic_lead_score_c > 99 THEN 20
            WHEN firmographic_demographic_lead_score_c > 49 THEN 10
            ELSE 0 
        END AS firm_score_add,
        lead_score_engagement_profile_c AS lead_score_engagement_profile,
        ft_utm_medium_c AS medium_first_touch,
        ft_utm_source_c AS source_first_touch,
        ft_utm_campaign_c AS campaign_first_touch,
        channel_lead_creation_c AS lead_channel_forecast,
        email_bounced_reason,
        email_bounced_reason_new_c AS email_bounced_reason_new,
        legitimate_basis_c AS legitimate_basis,
        email_bounced_date,
        email_bounced_date_c AS email_bounced_date_new,
        CASE 
            WHEN LOWER (source_lead_creation_c) = 'predates attribution'
            THEN lt_utm_source_c
        END AS source_lead_creation, 
        campaign_lead_creation_c AS campaign_lead_creation,
        firmographic_demographic_lead_score_c AS firmographic_demographic_lead_score,
        base.sales_loft_most_recent_cadence_name_c AS most_recent_salesloft_cadence,
        base.looking_for_ma_c AS looking_for_ma,
        do_not_contact_c AS do_not_contact,
        form_consent_opt_in_c AS form_consent_opt_in,
        base.data_enrich_employee_count_c AS employee_count,
        CASE 
            WHEN annual_revenue <= 49999999 THEN 'SMB'
            WHEN annual_revenue > 49999999 AND annual_revenue <= 499999999 THEN 'Mid-Market'
            WHEN annual_revenue > 499999999 THEN 'Enterprise'
        END AS company_size_rev,
        CASE 
            WHEN LOWER(channel_lead_creation) = 'organic' THEN 'Organic'
            WHEN LOWER(channel_lead_creation) = 'direct mail' THEN 'Direct Mail'
            WHEN LOWER(channel_lead_creation) = 'phone' THEN 'Phone'
            WHEN LOWER(channel_lead_creation) IS null AND medium_lead_creation IS null AND source_lead_creation IS null THEN 'Unknown'
            WHEN LOWER(channel_lead_creation) = 'product' AND LOWER(medium_lead_creation) = 'product - login' THEN 'Product - Lead'
            WHEN LOWER(channel_lead_creation) = 'social' AND LOWER(medium_lead_creation) = 'social-organic' THEN 'Social - Organic'
            WHEN LOWER(channel_lead_creation) = 'social' AND LOWER(medium_lead_creation) = 'social_organic' THEN 'Social - Organic'
            WHEN LOWER(channel_lead_creation) = 'social' AND LOWER(medium_lead_creation) = 'social-paid' THEN 'Paid Social'
            WHEN LOWER(channel_lead_creation) = 'social' AND LOWER(medium_lead_creation) = 'paid-social' THEN 'Paid Social'
            WHEN LOWER(channel_lead_creation) IS null AND LOWER(medium_lead_creation) = 'social-paid' THEN 'Paid Social'
            WHEN LOWER(channel_lead_creation) = 'ppc' AND LOWER(medium_lead_creation) = 'cpc' THEN 'PPC - Display'
            WHEN LOWER(channel_lead_creation) = 'ppc' AND LOWER(medium_lead_creation) = 'display' THEN 'PPC - Display'
            WHEN LOWER(channel_lead_creation) = 'ppc' AND LOWER(medium_lead_creation) = 'intent partner' THEN 'PPC - Intent'
            WHEN LOWER(channel_lead_creation) = 'ppc' AND LOWER(medium_lead_creation) = 'search' THEN 'PPC - General'
            WHEN LOWER(channel_lead_creation) = 'ppc' AND LOWER(medium_lead_creation) = 'social-paid' THEN 'Paid Social'
            WHEN LOWER(channel_lead_creation) = 'ppc' AND LOWER(medium_lead_creation) IS NOT null THEN 'PPC - General'
            WHEN LOWER(channel_lead_creation) = 'ppc' AND LOWER(medium_lead_creation) IS null THEN 'PPC - General'
            WHEN LOWER(channel_lead_creation) = 'email' AND LOWER(source_lead_creation) like '%act-on%' THEN 'Email - Paid' 
            WHEN LOWER(channel_lead_creation) = 'email' AND LOWER(medium_lead_creation) = 'email_paid' THEN 'Email - Paid'
            WHEN LOWER(channel_lead_creation) = 'email' AND LOWER(medium_lead_creation) = 'email-paid' THEN 'Email - Paid'
            WHEN LOWER(channel_lead_creation) = 'email' AND LOWER(medium_lead_creation) = 'email- paid' THEN 'Email - Paid' 
            WHEN LOWER(channel_lead_creation) = 'email' AND LOWER(medium_lead_creation) = 'email_inhouse' THEN 'Email - Paid' 
            WHEN LOWER(channel_lead_creation) = 'email' AND LOWER(medium_lead_creation) = 'paid-email' THEN 'Email - Paid' 
            WHEN LOWER(channel_lead_creation) = 'email' AND LOWER(medium_lead_creation) = 'email' THEN 'Email - Paid' 
            WHEN LOWER(channel_lead_creation) = 'email' AND LOWER(medium_lead_creation) = 'syndication_partner' THEN 'Email - Paid' 
            WHEN LOWER(channel_lead_creation) = 'ppl' AND LOWER(medium_lead_creation) = 'syndication partner' THEN 'PPL - Syndication'  
            WHEN LOWER(channel_lead_creation) = 'ppl' AND LOWER(medium_lead_creation) = 'intent partner' THEN 'PPL - Intent'
            WHEN LOWER(channel_lead_creation) = 'ppl' THEN 'PPL'
            WHEN LOWER(channel_lead_creation) = 'prospecting' AND LOWER(medium_lead_creation) = 'intent partner' THEN 'Prospecting - Intent Partners'
            WHEN LOWER(channel_lead_creation) = 'events' AND LOWER(medium_lead_creation) = 'webinar' THEN 'Webinar'
            WHEN LOWER(channel_lead_creation) IN ('event','events') THEN 'Events and Trade Shows'
            WHEN LOWER(channel_lead_creation) = 'partner' THEN 'Partners'
            WHEN LOWER(medium_lead_creation) = 'partner' THEN 'Partners'
            WHEN LOWER(medium_lead_creation) = 'virtualevent' THEN 'Events and Trade Shows'
            WHEN LOWER(channel_lead_creation) = 'prospecting' AND LOWER(medium_lead_creation) = 'sdr' THEN 'Prospecting - SDR'
            WHEN LOWER(channel_lead_creation) = 'prospecting' AND LOWER(medium_lead_creation) = 'rsm' THEN 'Prospecting - RSM'
            WHEN LOWER(channel_lead_creation) = 'prospecting' AND LOWER(medium_lead_creation) = 'channel management' THEN 'Prospecting - Channel'
            WHEN LOWER(channel_lead_creation) = 'prospecting' AND LOWER(medium_lead_creation) = 'third-party' THEN 'Prospecting - Third Party'
            WHEN LOWER(channel_lead_creation) = 'prospecting' THEN 'Prospecting - General'
            WHEN LOWER(channel_lead_creation) = 'referral' THEN 'Referral - General'
            WHEN LOWER(channel_lead_creation) = 'predates attribution' AND LOWER(medium_lead_creation) = 'predates attribution' THEN 'Predates Attribution'
            ELSE 'Other'
        END AS channel_bucket_details,
        CASE 
            WHEN LOWER(channel_lead_creation) = 'organic' THEN 'Organic'
            WHEN LOWER(channel_lead_creation) = 'direct mail' THEN 'Direct Mail'
            WHEN LOWER(channel_lead_creation) = 'phone' THEN 'Phone'
            WHEN LOWER(channel_lead_creation) IS null AND medium_lead_creation IS null AND source_lead_creation IS null THEN 'Unknown'
            WHEN LOWER(channel_lead_creation) = 'product' AND LOWER(medium_lead_creation) = 'product - login' THEN 'Product'
            WHEN LOWER(channel_lead_creation) = 'social' AND LOWER(medium_lead_creation) = 'social-organic' THEN 'Social'
            WHEN LOWER(channel_lead_creation) = 'social' AND LOWER(medium_lead_creation) = 'social_organic' THEN 'Social'
            WHEN LOWER(channel_lead_creation) = 'social' AND LOWER(medium_lead_creation) = 'social-paid' THEN 'Paid Social'
            WHEN LOWER(channel_lead_creation) = 'social' AND LOWER(medium_lead_creation) = 'paid-social' THEN 'Paid Social'
            WHEN LOWER(channel_lead_creation) IS null AND LOWER(medium_lead_creation) = 'social-paid' THEN 'Paid Social'
            WHEN LOWER(channel_lead_creation) = 'ppc' AND LOWER(medium_lead_creation) = 'cpc' THEN 'PPC'
            WHEN LOWER(channel_lead_creation) = 'ppc' AND LOWER(medium_lead_creation) = 'display' THEN 'PPC'
            WHEN LOWER(channel_lead_creation) = 'ppc' AND LOWER(medium_lead_creation) = 'intent partner' THEN 'PPC'
            WHEN LOWER(channel_lead_creation) = 'ppc' AND LOWER(medium_lead_creation) = 'search' THEN 'PPC'
            WHEN LOWER(channel_lead_creation) = 'ppc' AND LOWER(medium_lead_creation) = 'social-paid' THEN 'PPC'
            WHEN LOWER(channel_lead_creation) = 'ppc' AND LOWER(medium_lead_creation) IS NOT null THEN 'PPC'
            WHEN LOWER(channel_lead_creation) = 'ppc' AND LOWER(medium_lead_creation) IS null THEN 'PPC'
            WHEN LOWER(channel_lead_creation) = 'email' AND LOWER(source_lead_creation) like '%act-on%' THEN 'Email' 
            WHEN LOWER(channel_lead_creation) = 'email' AND LOWER(medium_lead_creation) = 'email_paid' THEN 'Email'
            WHEN LOWER(channel_lead_creation) = 'email' AND LOWER(medium_lead_creation) = 'email-paid' THEN 'Email'
            WHEN LOWER(channel_lead_creation) = 'email' AND LOWER(medium_lead_creation) = 'email- paid' THEN 'Email' 
            WHEN LOWER(channel_lead_creation) = 'email' AND LOWER(medium_lead_creation) = 'email_inhouse' THEN 'Email' 
            WHEN LOWER(channel_lead_creation) = 'email' AND LOWER(medium_lead_creation) = 'paid-email' THEN 'Email' 
            WHEN LOWER(channel_lead_creation) = 'email' AND LOWER(medium_lead_creation) = 'email' THEN 'Email' 
            WHEN LOWER(channel_lead_creation) = 'email' AND LOWER(medium_lead_creation) = 'syndication_partner' THEN 'Email' 
            WHEN LOWER(channel_lead_creation) = 'ppl' AND LOWER(medium_lead_creation) = 'syndication partner' THEN 'PPL'  
            WHEN LOWER(channel_lead_creation) = 'ppl' AND LOWER(medium_lead_creation) = 'intent partner' THEN 'PPL'
            WHEN LOWER(channel_lead_creation) = 'ppl' THEN 'PPL'
            WHEN LOWER(channel_lead_creation) = 'prospecting' AND LOWER(medium_lead_creation) = 'intent partner' THEN 'Prospecting'
            WHEN LOWER(channel_lead_creation) = 'events' AND LOWER(medium_lead_creation) = 'webinar' THEN 'Webinar'
            WHEN LOWER(channel_lead_creation) IN ('event','events') THEN 'Event'
            WHEN LOWER(channel_lead_creation) = 'partner' THEN 'Partner'
            WHEN LOWER(medium_lead_creation) = 'partner' THEN 'Partner'
            WHEN LOWER(medium_lead_creation) = 'virtualevent' THEN 'Event'
            WHEN LOWER(channel_lead_creation) = 'prospecting' AND LOWER(medium_lead_creation) = 'sdr' THEN 'Prospecting'
            WHEN LOWER(channel_lead_creation) = 'prospecting' AND LOWER(medium_lead_creation) = 'rsm' THEN 'Prospecting'
            WHEN LOWER(channel_lead_creation) = 'prospecting' AND LOWER(medium_lead_creation) = 'channel management' THEN 'Prospecting'
            WHEN LOWER(channel_lead_creation) = 'prospecting' AND LOWER(medium_lead_creation) = 'third-party' THEN 'Prospecting'
            WHEN LOWER(channel_lead_creation) = 'prospecting' THEN 'Prospecting'
            WHEN LOWER(channel_lead_creation) = 'referral' THEN 'Partner_Referral'
            WHEN LOWER(channel_lead_creation) = 'predates attribution' AND LOWER(medium_lead_creation) = 'predates attribution' THEN 'Predates Attribution'
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
        COALESCE(account_c,lean_data_a_2_b_account_c) AS person_account_id,
        CASE 
            WHEN LOWER(lt_utm_channel_c) = 'organic' THEN 'Organic'
            WHEN LOWER(lt_utm_channel_c) = 'direct mail' THEN 'Direct Mail'
            WHEN LOWER(channel_lead_creation_c) = 'phone' THEN 'Phone'
            WHEN LOWER(lt_utm_channel_c) IS null AND lt_utm_medium_c IS null AND lt_utm_source_c IS null THEN 'Unknown'
            WHEN LOWER(lt_utm_channel_c) = 'product' AND LOWER(lt_utm_medium_c) = 'product - login' THEN 'Product'
            WHEN LOWER(lt_utm_channel_c) = 'social' AND LOWER(lt_utm_medium_c) = 'social-organic' THEN 'Social'
            WHEN LOWER(lt_utm_channel_c) = 'social' AND LOWER(lt_utm_medium_c) = 'social_organic' THEN 'Social'
            WHEN LOWER(lt_utm_channel_c) = 'social' AND LOWER(lt_utm_medium_c) = 'social-paid' THEN 'Paid Social'
            WHEN LOWER(lt_utm_channel_c) = 'social' AND LOWER(lt_utm_medium_c) = 'paid-social' THEN 'Paid Social'
            WHEN LOWER(lt_utm_channel_c) IS null AND LOWER(lt_utm_medium_c) = 'social-paid' THEN 'Organic Social'
            WHEN LOWER(lt_utm_channel_c) = 'ppc' AND LOWER(lt_utm_medium_c) = 'cpc' THEN 'PPC'
            WHEN LOWER(lt_utm_channel_c) = 'ppc' AND LOWER(lt_utm_medium_c) = 'display' THEN 'PPC'
            WHEN LOWER(lt_utm_channel_c) = 'ppc' AND LOWER(lt_utm_medium_c) = 'intent partner' THEN 'PPC'
            WHEN LOWER(lt_utm_channel_c) = 'ppc' AND LOWER(lt_utm_medium_c) = 'search' THEN 'Paid Search'
            WHEN LOWER(lt_utm_channel_c) = 'ppc' AND LOWER(lt_utm_medium_c) = 'social-paid' THEN 'PPC'
            WHEN LOWER(lt_utm_channel_c) = 'ppc' AND LOWER(lt_utm_medium_c) IS NOT null THEN 'PPC'
            WHEN LOWER(lt_utm_channel_c) = 'ppc' AND LOWER(lt_utm_medium_c) IS null THEN 'PPC'
            WHEN LOWER(lt_utm_channel_c) = 'email' AND LOWER(lt_utm_source_c) like '%act-on%' THEN 'Email' 
            WHEN LOWER(lt_utm_channel_c) = 'email' AND LOWER(lt_utm_medium_c) = 'email_paid' THEN 'Email'
            WHEN LOWER(lt_utm_channel_c) = 'email' AND LOWER(lt_utm_medium_c) = 'email-paid' THEN 'Email'
            WHEN LOWER(lt_utm_channel_c) = 'email' AND LOWER(lt_utm_medium_c) = 'email- paid' THEN 'Email' 
            WHEN LOWER(lt_utm_channel_c) = 'email' AND LOWER(lt_utm_medium_c) = 'email_inhouse' THEN 'Email' 
            WHEN LOWER(lt_utm_channel_c) = 'email' AND LOWER(lt_utm_medium_c) = 'paid-email' THEN 'Email' 
            WHEN LOWER(lt_utm_channel_c) = 'email' AND LOWER(lt_utm_medium_c) = 'email' THEN 'Email' 
            WHEN LOWER(lt_utm_channel_c) = 'email' AND LOWER(lt_utm_medium_c) = 'syndication_partner' THEN 'Email' 
            WHEN LOWER(lt_utm_channel_c) = 'ppl' AND LOWER(lt_utm_medium_c) = 'syndication partner' THEN 'PPL'  
            WHEN LOWER(lt_utm_channel_c) = 'ppl' AND LOWER(lt_utm_medium_c) = 'intent partner' THEN 'PPL'
            WHEN LOWER(lt_utm_channel_c) = 'ppl' THEN 'PPL'
            WHEN LOWER(lt_utm_channel_c) = 'prospecting' AND LOWER(lt_utm_medium_c) = 'intent partner' THEN 'Prospecting'
            WHEN LOWER(lt_utm_channel_c) = 'events' AND LOWER(lt_utm_medium_c) = 'webinar' THEN 'Webinar'
            WHEN LOWER(lt_utm_channel_c) IN ('event','events') THEN 'Event'
            WHEN LOWER(lt_utm_channel_c) = 'partner' THEN 'Partner'
            WHEN LOWER(lt_utm_medium_c) = 'partner' THEN 'Partner'
            WHEN LOWER(lt_utm_medium_c) = 'virtualevent' THEN 'Event'
            WHEN LOWER(lt_utm_channel_c) = 'prospecting' AND LOWER(lt_utm_medium_c) = 'sdr' THEN 'Prospecting'
            WHEN LOWER(lt_utm_channel_c) = 'prospecting' AND LOWER(lt_utm_medium_c) = 'rsm' THEN 'Prospecting'
            WHEN LOWER(lt_utm_channel_c) = 'prospecting' AND LOWER(lt_utm_medium_c) = 'channel management' THEN 'Prospecting'
            WHEN LOWER(lt_utm_channel_c) = 'prospecting' AND LOWER(lt_utm_medium_c) = 'third-party' THEN 'Prospecting'
            WHEN LOWER(lt_utm_channel_c) = 'prospecting' THEN 'Prospecting'
            WHEN LOWER(lt_utm_channel_c) = 'referral' THEN 'Partner_Referral'
            WHEN LOWER(lt_utm_channel_c) = 'predates attribution' AND LOWER(lt_utm_medium_c) = 'predates attribution' THEN 'Predates Attribution'
            ELSE 'Other'
        END AS channel_bucket_lt,
        abm_campaign_initial_c AS abm_campaign_initial,
        abm_campaign_most_recent_c AS abm_campaign_most_recent,
        abm_date_time_initial_c AS abm_date_time_initial,
        abm_date_time_most_recent_c AS abm_date_time_most_recent,
        is_abm_c AS is_abm
    FROM base
    LEFT JOIN {{ref('user_source_xf')}} creator ON
    base.created_by_id=creator.user_id
    WHERE base.owner_id != '00Ga0000003Nugr' -- AO-Fake Leads
    AND base.is_deleted = 'False'

), lead_score_prep AS (

    SELECT
        final.*,
        CASE
            WHEN lead_score_9883 > 19 THEN SUM(firm_score_add + lead_score_9883 + lead_score_engagement_profile)
            ELSE lead_score_9883
        END AS combined_lead_score
    FROM final
    {{dbt_utils.group_by(n=100)}}
)

SELECT 
    lead_score_prep.*,
    CASE
        WHEN global_region IN ('EUROPE','ROW','AUNZ') THEN 'EMEA'
        WHEN company_size_rev IN ('SMB') OR company_size_rev IS null THEN 'Velocity'
        WHEN company_size_rev IN ('Mid-Market','Enterprise') THEN 'Upmarket'
        ELSE null
    END AS segment,
    CASE
        WHEN lead_status = 'Current Customer' THEN true
        ELSE False
    END AS is_current_customer,
    SUM (combined_lead_score - de_ec_subtraction) AS lead_score
FROM lead_score_prep
{{dbt_utils.group_by(n=103)}}