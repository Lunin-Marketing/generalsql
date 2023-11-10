{{ config(materialized='table') }}

WITH base AS (

SELECT *
FROM {{ source('salesforce', 'contact') }}

), final AS (

    SELECT DISTINCT
        base.id AS contact_id,
        base.is_deleted,
        base.created_by_id,
        creator.user_name AS created_by_name,
        base.account_id,
        account_source_xf.account_name,
        base.first_name,
        base.mailing_postal_code,
        base.mailing_country,
        base.email,
        base.title,
        base.lead_source,
        base.de_department_c AS department,
        DATE_TRUNC('day',base.created_date)::Date AS created_date,
        DATE_TRUNC('day',base.hand_raiser_date_time_c)::Date AS hand_raiser_date,
        DATE_TRUNC('day',base.last_activity_date)::Date AS last_activity_date,
        base.last_modified_date,
        base.system_modstamp AS systemmodstamp,
        account_source_xf.is_current_customer AS is_current_customer,
        base.no_longer_with_company_c AS is_no_longer_with_company,
        base.hand_raiser_c AS is_hand_raiser,
        base.mql_created_date_c AS mql_created_date,
        base.mql_most_recent_date_c AS mql_most_recent_date,
        base.contact_role_c AS contact_role,
        base.primary_contact_c AS is_primary_contact,
        base.ft_offer_asset_type_c AS offer_asset_type_first_touch,
        base.ft_offer_asset_subtype_c AS offer_asset_subtype_first_touch,
        base.contact_status_c AS contact_status,
        base.ft_utm_channel_c AS channel_first_touch,
        base.lt_utm_channel_c AS channel_last_touch,
        base.lt_utm_medium_c AS medium_last_touch,
        base.lt_utm_source_c AS source_last_touch,
        base.lt_utm_campaign_c AS campaign_last_touch,
        CASE 
            WHEN LOWER (channel_lead_creation_c) = 'predates attribution'
            THEN lt_utm_channel_c
        END AS channel_lead_creation,
        base.campaign_lead_creation_c AS campaign_lead_creation,
        base.content_lead_creation_c AS content_lead_creation,
        base.term_lead_creation_c AS term_lead_creation,
        base.ft_offer_asset_topic_c AS offer_asset_topic_first_touch,
        base.ft_offer_asset_name_c AS offer_asset_name_first_touch,
        base.lc_offer_asset_type_c AS offer_asset_type_lead_creation,
        base.lc_offer_asset_subtype_c AS offer_asset_subtype_lead_creation,
        base.lc_offer_asset_topic_c AS offer_asset_topic_lead_creation,
        base.lc_offer_asset_name_c AS offer_asset_name_lead_creation,
        base.lt_offer_asset_type_c AS offer_asset_type_last_touch,
        base.lt_offer_asset_subtype_c AS offer_asset_subtype_last_touch,
        base.lt_offer_asset_name_c AS offer_asset_name_last_touch,
        base.lt_offer_asset_topic_c AS offer_asset_topic_last_touch,
        base.renewal_contact_c AS is_renewal_contact, --verify data type
        base.account_owner_email_c AS account_owner_email,
        base.account_csm_email_c AS account_csm_email,
        base.ft_subchannel_c AS subchannel_first_touch,
        base.lt_subchannel_c AS subchannel_last_touch,
        base.lc_subchannel_c AS subchannel_lead_creation,
        base.x_9883_lead_score_c AS lead_score_9883,
        CASE
            WHEN account_source_xf.number_of_employees = 0 THEN 20
            ELSE 0        
        END AS de_ec_subtraction,
        CASE
            WHEN base.firmographic_demographic_lead_score_c > 99 THEN 20
            WHEN base.firmographic_demographic_lead_score_c > 49 THEN 10
            ELSE 0 
        END AS firm_score_add,
        lead_score_engagement_profile_c AS lead_score_engagement_profile,
        base.status_reason_c AS status_reason,
        DATE_TRUNC('day',marketing_lead_creation_date_c)::Date AS marketing_created_date,
        base.current_map_c AS current_ma,
        base.account_lookup_c AS account_lookup,
        base.ft_utm_campaign_c AS campaign_first_touch,
        base.ft_utm_medium_c AS medium_first_touch,
        base.ft_utm_source_c AS source_first_touch,
        base.lead_id_converted_from_c AS lead_id_converted_from,
        base.was_a_handraiser_lead_c AS was_a_handraiser_lead,
        CASE 
            WHEN LOWER (medium_lead_creation_c) = 'predates attribution'
            THEN lt_utm_medium_c
        END AS medium_lead_creation, 
        CASE   
            WHEN LOWER (source_lead_creation_c) = 'predates attribution'
            THEN lt_utm_source_c
        END AS source_lead_creation,
        base.form_consent_opt_in_c AS form_consent_opt_in,
        base.owner_id AS contact_owner_id,
        base.firmographic_demographic_lead_score_c AS firmographic_demographic_lead_score,
        base.last_name, 
        base.data_enrich_company_name_c AS de_account_name,
        base.email_bounced_date_c AS email_bounced_date_new,
        base.email_bounced_reason_new_c AS email_bounced_reason_new,
        DATE_TRUNC('day',date_time_to_working_c)::Date AS working_date,
        base.looking_for_ma_c AS looking_for_ma,
        account_source_xf.account_owner_id,
        account_source_xf.account_owner_name,
        account_source_xf.annual_revenue,
        account_source_xf.de_current_crm,
        account_source_xf.de_current_ma,
        account_source_xf.account_csm_name AS account_csm_name,
        account_source_xf.csm_id AS account_csm_id,
        account_source_xf.sdr_phone AS account_sdr_phone,
        account_source_xf.sdr_photo AS account_sdr_photo,
        account_source_xf.sdr_calendly AS account_sdr_calendly,
        account_source_xf.sdr_title AS account_sdr_title,
        account_source_xf.sdr_full_name AS account_sdr_full_name,
        account_source_xf.sdr_email AS account_sdr_email,
        account_source_xf.account_deliverability_consultant_email,
        account_source_xf.account_deliverability_consultant,
        user_source_xf.user_email AS owner_email,
        account_source_xf.target_account,
        account_source_xf.number_of_employees AS employee_count,
        base.sales_loft_most_recent_cadence_name_c AS most_recent_salesloft_cadence,
        CASE
            WHEN account_source_xf.annual_revenue <= 49999999 THEN 'SMB'
            WHEN account_source_xf.annual_revenue > 49999999 AND account_source_xf.annual_revenue <= 499999999 THEN 'Mid-Market'
            WHEN account_source_xf.annual_revenue > 499999999 THEN 'Enterprise'
        END AS company_size_rev,
        account_source_xf.global_region,
        account_source_xf.segment,
        account_source_xf.de_industry AS industry,
        CASE
            WHEN account_source_xf.de_industry IN ('Advertising & Marketing') THEN 'Advertising & Marketing'
            WHEN account_source_xf.de_industry IN ('Agriculture','Animals & Livestock','Crops','Forestry') THEN 'Agriculture'
            WHEN account_source_xf.de_industry IN ('Business Services','Call Centers & Business Centers','Chambers of Commerce','Commercial Printing','Debt Collection','Facilities Management & Commercial Cleaning','Food Service','HR & Staffing','Human Resources & Staffing','Information & Document Management','Management Consulting','Multimedia & Graphic Design','Research & Development','Security Products & Services','Translation & Linguistic Services') THEN 'Business Services'
            WHEN account_source_xf.de_industry IN ('Architecture, Engineering & Design','Civil Engineering Construction','Commercial & Residential Construction','Construction') THEN 'Construction'
            WHEN account_source_xf.de_industry IN ('Automotive Service & Collision Repair','Barber Shops & Beauty Salons','Childcare','Cleaning Services','Consumer Services','Funeral Homes & Funeral Related Services','Photography Studio','Repair Services','Weight & Health Management') THEN 'Consumer Services'
            WHEN account_source_xf.de_industry IN ('Colleges & Universities','Education','K-12 Schools','Training') THEN 'Education'
            WHEN account_source_xf.de_industry IN ('Electricity, Oil & Gas','Energy, Utilities & Waste','Energy, Utilities & Waste Treatment','Oil & Gas Exploration & Services','Waste Treatment, Environmental Services & Recycling','Water & Water Treatment','Water Treatment','Energy/Utilities') THEN 'Energy/Utilities'
            WHEN account_source_xf.de_industry IN ('Finance','Insurance','Banking','Brokerage','Credit Cards & Transaction Processing','Investment Banking','Lending & Brokerage','Venture Capital & Private Equity') THEN 'Finance'
            WHEN account_source_xf.de_industry IN ('Federal','Government','State','Tribal Nations') THEN 'Government'
            WHEN account_source_xf.de_industry IN ('Ambulance Services','Blood & Organ Banks','Elderly Care Services','Healthcare Services','Medical Laboratories & Imaging Centers','Mental Health & Rehabilitation Facilities','Veterinary Services') THEN 'Healthcare Services'
            WHEN account_source_xf.de_industry IN ('Holding Companies & Conglomerates') THEN 'Holding Companies'
            WHEN account_source_xf.de_industry IN ('Amusement Parks, Arcades & Attractions','Cultural & Informational Centers','Fitness & Dance Facilities','Gambling & Gaming','Hospitality','Libraries','Lodging & Resorts','Movie Theaters','Museums & Art Galleries','Performing Arts Theaters','Restaurants','Sports Teams & Leagues','Travel Agencies & Services','Zoos & National Parks') THEN 'Hospitality'
            WHEN account_source_xf.de_industry IN ('Dental Offices','Hospitals & Physicians Clinics','Medical & Surgical Hospitals','Medical Specialists','Physicians Clinics') THEN 'Hospitals/Clinics'
            WHEN account_source_xf.de_industry IN ('Manufacturing','Aerospace & Defense','Appliances','Automotive Parts','Boats & Submarines','Building Materials','Chemicals & Related Products','Chemicals, Petrochemicals, Glass & Gases','Cleaning Products','Computer Equipment & Peripherals','Cosmetics, Beauty Supply & Personal Care Products','Electronics','Food & Beverage','Food, Beverages & Tobacco','Furniture','Glass & Clay','Hand, Power & Lawn-care Tools','Health & Nutrition Products','Household Goods','Industrial Machinery & Equipment','Medical Devices & Equipment','Motor Vehicle Parts','Motor Vehicles','Pet Products','Pharmaceuticals','Photographic & Optical Equipment','Plastic, Packaging & Containers','Pulp & Paper','Sporting Goods','Telecommunication Equipment','Test & Measurement Equipment','Textiles & Apparel','Tires & Rubber','Toys & Games','Watches & Jewelry','Wire & Cable') THEN 'Manufacturing'
            WHEN account_source_xf.de_industry IN ('Broadcasting','Data Collection & Internet Portals','Media & Internet','Music & Music Related Services','Music Production & Services','Newspapers & News Services','Publishing','Social Networks','Ticket Sales') THEN 'Media & Internet'
            WHEN account_source_xf.de_industry IN ('Metals & Mining') THEN 'Metals & Mining'
            WHEN account_source_xf.de_industry IN ('Minerals & Mining') THEN 'Minerals & Mining'
            WHEN account_source_xf.de_industry IN ('Membership Organizations','Non-Profit & Charitable Organizations','Organizations','Religious Organizations') THEN 'Organizations'
            WHEN account_source_xf.de_industry IN ('Accounting & Accounting Services','Accounting Services','Law Firms & Legal Services') THEN 'Professional Services'
            WHEN account_source_xf.de_industry IN ('Real Estate') THEN 'Real Estate'
            WHEN account_source_xf.de_industry IN ('Apparel & Accessories Retail','Auctions','Automobile Dealers','Automobile Parts Stores','Car & Truck Rental','Consumer Electronics & Computers Retail','Convenience Stores, Gas Stations & Liquor Stores','Department Stores, Shopping Centers & Superstores','Drug Stores & Pharmacies','Flowers, Gifts & Specialty Stores','Grocery Retail','Home Improvement & Hardware Retail','Jewelry & Watch Retail','Office Products Retail & Distribution','Other Rental Stores (Furniture, A/V, Construction & Industrial Equipment)','Record, Video & Book Stores','Retail','Sporting & Recreational Equipment Retail','Vitamins, Supplements & Health Stores') THEN 'Retail'
            WHEN account_source_xf.de_industry IN ('Business Intelligence (BI) Software','Content & Collaboration Software','Custom Software & IT Services','Customer Relationship Management (CRM) Software','Database & File Management Software','Engineering Software','Enterprise Resource Planning (ERP) Software','Financial Software','Healthcare Software','Human Resources Software','Legal Software','Mobile App Development','Multimedia, Games & Graphics Software','Networking Software','Security Software','Storage & System Management Software','Supply Chain Management (SCM) Software','Software') THEN 'Software'
            WHEN account_source_xf.de_industry IN ('Telecommunications','Cable & Satellite','Internet Service Providers, Website Hosting & Internet-related Services','Telephony & Wireless') THEN 'Telecommunications'
            WHEN account_source_xf.de_industry IN ('Airlines, Airports & Air Services','Freight & Logistics Services','Rail, Bus & Taxi','Trucking, Moving & Storage','Transportation','Marine Shipping & Transportation') THEN 'Transportation'
            ELSE 'Other'
        END AS industry_bucket,
        CASE 
            WHEN LOWER(channel_lead_creation_c) = 'organic' THEN 'Organic'
            WHEN LOWER(channel_lead_creation_c) = 'direct mail' THEN 'Direct Mail'
            WHEN LOWER(channel_lead_creation_c) = 'phone' THEN 'Phone'
            WHEN LOWER(channel_lead_creation_c) IS null AND medium_lead_creation_c IS null AND source_lead_creation_c IS null THEN 'Unknown'
            WHEN LOWER(channel_lead_creation_c) = 'product' AND LOWER(medium_lead_creation_c) = 'product - login' THEN 'Product - Lead'
            WHEN LOWER(channel_lead_creation_c) = 'social' AND LOWER(medium_lead_creation_c) = 'social-organic' THEN 'Social - Organic'
            WHEN LOWER(channel_lead_creation_c) = 'social' AND LOWER(medium_lead_creation_c) = 'social_organic' THEN 'Social - Organic'
            WHEN LOWER(channel_lead_creation_c) = 'social' AND LOWER(medium_lead_creation_c) = 'social-paid' THEN 'Paid Social'
            WHEN LOWER(channel_lead_creation_c) = 'social' AND LOWER(medium_lead_creation_c) = 'paid-social' THEN 'Paid Social'
            WHEN LOWER(channel_lead_creation_c) IS null AND LOWER(medium_lead_creation_c) = 'social-paid' THEN 'Paid Social'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) = 'cpc' THEN 'PPC - Display'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) = 'display' THEN 'PPC - Display'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) = 'intent partner' THEN 'PPC - Intent'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) = 'search' THEN 'PPC - General'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) = 'social-paid' THEN 'Paid Social'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) IS NOT null THEN 'PPC - General'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) IS null THEN 'PPC - General'
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
            WHEN LOWER(medium_lead_creation_c) = 'partner' THEN 'Partners'
            WHEN LOWER(medium_lead_creation_c) = 'virtualevent' THEN 'Events and Trade Shows'
            WHEN LOWER(channel_lead_creation_c) = 'prospecting' AND LOWER(medium_lead_creation_c) = 'sdr' THEN 'Prospecting - SDR'
            WHEN LOWER(channel_lead_creation_c) = 'prospecting' AND LOWER(medium_lead_creation_c) = 'rsm' THEN 'Prospecting - RSM'
            WHEN LOWER(channel_lead_creation_c) = 'prospecting' AND LOWER(medium_lead_creation_c) = 'channel management' THEN 'Prospecting - Channel'
            WHEN LOWER(channel_lead_creation_c) = 'prospecting' AND LOWER(medium_lead_creation_c) = 'third-party' THEN 'Prospecting - Third Party'
            WHEN LOWER(channel_lead_creation_c) = 'prospecting' THEN 'Prospecting - General'
            WHEN LOWER(channel_lead_creation_c) = 'referral' THEN 'Referral - General'
            WHEN LOWER(channel_lead_creation_c) = 'predates attribution' AND LOWER(medium_lead_creation_c) = 'predates attribution' THEN 'Predates Attribution'
            ELSE 'Other'
        END AS channel_bucket_details,
        CASE 
            WHEN LOWER(channel_lead_creation_c) = 'organic' THEN 'Organic'
            WHEN LOWER(channel_lead_creation_c) = 'direct mail' THEN 'Direct Mail'
            WHEN LOWER(channel_lead_creation_c) = 'phone' THEN 'Phone'
            WHEN LOWER(channel_lead_creation_c) IS null AND medium_lead_creation_c IS null AND source_lead_creation_c IS null THEN 'Unknown'
            WHEN LOWER(channel_lead_creation_c) = 'product' AND LOWER(medium_lead_creation_c) = 'product - login' THEN 'Product'
            WHEN LOWER(channel_lead_creation_c) = 'social' AND LOWER(medium_lead_creation_c) = 'social-organic' THEN 'Social'
            WHEN LOWER(channel_lead_creation_c) = 'social' AND LOWER(medium_lead_creation_c) = 'social_organic' THEN 'Social'
            WHEN LOWER(channel_lead_creation_c) = 'social' AND LOWER(medium_lead_creation_c) = 'social-paid' THEN 'Paid Social'
            WHEN LOWER(channel_lead_creation_c) = 'social' AND LOWER(medium_lead_creation_c) = 'paid-social' THEN 'Paid Social'
            WHEN LOWER(channel_lead_creation_c) IS null AND LOWER(medium_lead_creation_c) = 'social-paid' THEN 'Paid Social'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) = 'cpc' THEN 'PPC'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) = 'display' THEN 'PPC'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) = 'intent partner' THEN 'PPC'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) = 'search' THEN 'PPC'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) = 'social-paid' THEN 'PPC'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) IS NOT null THEN 'PPC'
            WHEN LOWER(channel_lead_creation_c) = 'ppc' AND LOWER(medium_lead_creation_c) IS null THEN 'PPC'
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
            WHEN LOWER(channel_lead_creation_c) = 'events' AND LOWER(medium_lead_creation_c) = 'webinar' THEN 'Webinar'
            WHEN LOWER(channel_lead_creation_c) IN ('event','events') THEN 'Event'
            WHEN LOWER(channel_lead_creation_c) = 'partner' THEN 'Partner'
            WHEN LOWER(medium_lead_creation_c) = 'partner' THEN 'Partner'
            WHEN LOWER(medium_lead_creation_c) = 'virtualevent' THEN 'Event'
            WHEN LOWER(channel_lead_creation_c) = 'prospecting' AND LOWER(medium_lead_creation_c) = 'sdr' THEN 'Prospecting'
            WHEN LOWER(channel_lead_creation_c) = 'prospecting' AND LOWER(medium_lead_creation_c) = 'rsm' THEN 'Prospecting'
            WHEN LOWER(channel_lead_creation_c) = 'prospecting' AND LOWER(medium_lead_creation_c) = 'channel management' THEN 'Prospecting'
            WHEN LOWER(channel_lead_creation_c) = 'prospecting' AND LOWER(medium_lead_creation_c) = 'third-party' THEN 'Prospecting'
            WHEN LOWER(channel_lead_creation_c) = 'prospecting' THEN 'Prospecting'
            WHEN LOWER(channel_lead_creation_c) = 'referral' THEN 'Partner_Referral'
            WHEN LOWER(channel_lead_creation_c) = 'predates attribution' AND LOWER(medium_lead_creation_c) = 'predates attribution' THEN 'Predates Attribution'
            ELSE 'Other'
        END AS channel_bucket,
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
        base.abm_campaign_initial_c AS abm_campaign_initial,
        base.abm_campaign_most_recent_c AS abm_campaign_most_recent,
        base.abm_date_time_initial_c AS abm_date_time_initial,
        base.abm_date_time_most_recent_c AS abm_date_time_most_recent,
        base.is_abm_c AS is_abm,
        base._fivetran_synced AS updated_at
    FROM base
    LEFT JOIN {{ref('account_source_xf')}} ON
    base.account_id=account_source_xf.account_id
    LEFT JOIN {{ref('user_source_xf')}} ON
    base.owner_id=user_source_xf.user_id
    LEFT JOIN {{ref('user_source_xf')}} creator ON
    base.created_by_id=creator.user_id
    WHERE base.is_deleted = 'False'

), lead_score_prep AS (

    SELECT
        final.*,
        CASE
            WHEN lead_score_9883 > 19 THEN SUM(firm_score_add + lead_score_9883 + lead_score_engagement_profile)
            ELSE lead_score_9883
        END AS combined_lead_score
    FROM final
    {{dbt_utils.group_by(n=110)}}

)

SELECT DISTINCT
    lead_score_prep.*,
    contact_id||'-'||updated_at AS unique_contact_id,
    SUM (combined_lead_score - de_ec_subtraction) AS lead_score
FROM lead_score_prep
{{dbt_utils.group_by(n=112)}}