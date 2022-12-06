{{ config(materialized='table') }}
WITH base AS (

SELECT *
FROM {{ source('salesforce', 'account') }}

), final AS (

    SELECT
        base.id AS account_id,
        base.is_deleted,
        base.name AS account_name,
        base.type AS account_type,
        base.parent_id AS account_parent_id,
        base.billing_state,
        base.billing_postal_code,
        base.billing_country,
        base.industry,
        CASE
            WHEN base.industry IN ('Business Services') THEN 'Business Services'
            WHEN base.industry IN ('Finance','Insurance') THEN 'Finance'
            WHEN base.industry IN ('Manufacturing') THEN 'Manufacturing'
            WHEN base.industry IN ('Software','Telecommunications') THEN 'SoftCom'
            ELSE 'Other'
        END AS default_industry_bucket,
        base.annual_revenue,
        base.number_of_employees,
        base.owner_id AS account_owner_id,
        base.created_date AS account_created_date,
        base.last_modified_date,
        base.system_modstamp AS systemmodstamp,
        base.is_partner,
        base.current_customer_c AS is_current_customer,
        base.csm_c AS account_csm_c,
        base.current_crm_c AS current_crm,
        base.current_marketing_automation_c AS current_ma,
        base.de_country_c AS de_country,
        base.de_current_crm_c AS de_current_crm,
        base.de_current_marketing_automation_c AS de_current_ma,
        base.de_industry_c AS de_industry,
        CASE
            WHEN base.de_industry_c IN ('Advertising & Marketing') THEN 'Advertising & Marketing'
            WHEN base.de_industry_c IN ('Agriculture','Animals & Livestock','Crops','Forestry') THEN 'Agriculture'
            WHEN base.de_industry_c IN ('Business Services','Call Centers & Business Centers','Chambers of Commerce','Commercial Printing','Debt Collection','Facilities Management & Commercial Cleaning','Food Service','HR & Staffing','Human Resources & Staffing','Information & Document Management','Management Consulting','Multimedia & Graphic Design','Research & Development','Security Products & Services','Translation & Linguistic Services') THEN 'Business Services'
            WHEN base.de_industry_c IN ('Architecture, Engineering & Design','Civil Engineering Construction','Commercial & Residential Construction','Construction') THEN 'Construction'
            WHEN base.de_industry_c IN ('Automotive Service & Collision Repair','Barber Shops & Beauty Salons','Childcare','Cleaning Services','Consumer Services','Funeral Homes & Funeral Related Services','Photography Studio','Repair Services','Weight & Health Management') THEN 'Consumer Services'
            WHEN base.de_industry_c IN ('Colleges & Universities','Education','K-12 Schools','Training') THEN 'Education'
            WHEN base.de_industry_c IN ('Electricity, Oil & Gas','Energy, Utilities & Waste','Energy, Utilities & Waste Treatment','Oil & Gas Exploration & Services','Waste Treatment, Environmental Services & Recycling','Water & Water Treatment','Water Treatment','Energy/Utilities') THEN 'Energy/Utilities'
            WHEN base.de_industry_c IN ('Finance','Insurance','Banking','Brokerage','Credit Cards & Transaction Processing','Investment Banking','Lending & Brokerage','Venture Capital & Private Equity') THEN 'Finance'
            WHEN base.de_industry_c IN ('Federal','Government','State','Tribal Nations') THEN 'Government'
            WHEN base.de_industry_c IN ('Ambulance Services','Blood & Organ Banks','Elderly Care Services','Healthcare Services','Medical Laboratories & Imaging Centers','Mental Health & Rehabilitation Facilities','Veterinary Services') THEN 'Healthcare Services'
            WHEN base.de_industry_c IN ('Holding Companies & Conglomerates') THEN 'Holding Companies'
            WHEN base.de_industry_c IN ('Amusement Parks, Arcades & Attractions','Cultural & Informational Centers','Fitness & Dance Facilities','Gambling & Gaming','Hospitality','Libraries','Lodging & Resorts','Movie Theaters','Museums & Art Galleries','Performing Arts Theaters','Restaurants','Sports Teams & Leagues','Travel Agencies & Services','Zoos & National Parks') THEN 'Hospitality'
            WHEN base.de_industry_c IN ('Dental Offices','Hospitals & Physicians Clinics','Medical & Surgical Hospitals','Medical Specialists','Physicians Clinics') THEN 'Hospitals/Clinics'
            WHEN base.de_industry_c IN ('Manufacturing','Aerospace & Defense','Appliances','Automotive Parts','Boats & Submarines','Building Materials','Chemicals & Related Products','Chemicals, Petrochemicals, Glass & Gases','Cleaning Products','Computer Equipment & Peripherals','Cosmetics, Beauty Supply & Personal Care Products','Electronics','Food & Beverage','Food, Beverages & Tobacco','Furniture','Glass & Clay','Hand, Power & Lawn-care Tools','Health & Nutrition Products','Household Goods','Industrial Machinery & Equipment','Medical Devices & Equipment','Motor Vehicle Parts','Motor Vehicles','Pet Products','Pharmaceuticals','Photographic & Optical Equipment','Plastic, Packaging & Containers','Pulp & Paper','Sporting Goods','Telecommunication Equipment','Test & Measurement Equipment','Textiles & Apparel','Tires & Rubber','Toys & Games','Watches & Jewelry','Wire & Cable') THEN 'Manufacturing'
            WHEN base.de_industry_c IN ('Broadcasting','Data Collection & Internet Portals','Media & Internet','Music & Music Related Services','Music Production & Services','Newspapers & News Services','Publishing','Social Networks','Ticket Sales') THEN 'Media & Internet'
            WHEN base.de_industry_c IN ('Metals & Mining') THEN 'Metals & Mining'
            WHEN base.de_industry_c IN ('Minerals & Mining') THEN 'Minerals & Mining'
            WHEN base.de_industry_c IN ('Membership Organizations','Non-Profit & Charitable Organizations','Organizations','Religious Organizations') THEN 'Organizations'
            WHEN base.de_industry_c IN ('Accounting & Accounting Services','Accounting Services','Law Firms & Legal Services') THEN 'Professional Services'
            WHEN base.de_industry_c IN ('Real Estate') THEN 'Real Estate'
            WHEN base.de_industry_c IN ('Apparel & Accessories Retail','Auctions','Automobile Dealers','Automobile Parts Stores','Car & Truck Rental','Consumer Electronics & Computers Retail','Convenience Stores, Gas Stations & Liquor Stores','Department Stores, Shopping Centers & Superstores','Drug Stores & Pharmacies','Flowers, Gifts & Specialty Stores','Grocery Retail','Home Improvement & Hardware Retail','Jewelry & Watch Retail','Office Products Retail & Distribution','Other Rental Stores (Furniture, A/V, Construction & Industrial Equipment)','Record, Video & Book Stores','Retail','Sporting & Recreational Equipment Retail','Vitamins, Supplements & Health Stores') THEN 'Retail'
            WHEN base.de_industry_c IN ('Business Intelligence (BI) Software','Content & Collaboration Software','Custom Software & IT Services','Customer Relationship Management (CRM) Software','Database & File Management Software','Engineering Software','Enterprise Resource Planning (ERP) Software','Financial Software','Healthcare Software','Human Resources Software','Legal Software','Mobile App Development','Multimedia, Games & Graphics Software','Networking Software','Security Software','Storage & System Management Software','Supply Chain Management (SCM) Software','Software') THEN 'Software'
            WHEN base.de_industry_c IN ('Telecommunications','Cable & Satellite','Internet Service Providers, Website Hosting & Internet-related Services','Telephony & Wireless') THEN 'Telecommunications'
            WHEN base.de_industry_c IN ('Airlines, Airports & Air Services','Freight & Logistics Services','Rail, Bus & Taxi','Trucking, Moving & Storage','Transportation','Marine Shipping & Transportation') THEN 'Transportation'
            ELSE 'Other'
        END AS industry_bucket,
        base.de_parent_company_c AS de_account_parent_name,
        base.de_ultimate_parent_company_c AS de_ultimate_parent_account_name,
        base.ao_instance_number_c AS ao_instance_number,
        base.do_not_market_c AS do_not_market,
        base.target_account_c AS target_account,
        base.market_segment_static_c AS market_segment_static,
        base.sdr_c AS sdr,
        base.current_contract_c AS current_contract,
        base.onboarding_completion_date_c AS onboarding_completion_date,
        base.customer_since_c AS customer_since,
        base.onboarding_specialist_c AS onboarding_specialist,
        base.executive_sponsor_c AS executive_sponsor,
        base.assigned_account_tier_c AS assigned_account_tier,
        base.business_model_c AS business_model,
        base.contract_type_c AS contract_type,
        base.delivered_support_tier_c AS delivered_support_tier,
        parent.name AS account_parent_name,
        base.deliverability_consultant_c AS deliverability_consultant_id,
        sdr.user_email AS sdr_email,
        sdr.user_first_name AS sdr_first_name,
        sdr.user_full_name AS sdr_full_name,
        sdr.user_photo AS sdr_photo,
        sdr.calendly_link AS sdr_calendly,
        sdr.user_title AS sdr_title,
        sdr.user_phone AS sdr_phone,
        csm.user_email AS account_csm_email,
        csm.user_photo AS account_csm_photo,
        csm.user_id AS csm_id,
        csm.user_full_name AS account_csm_name,
        account_owner.user_full_name AS account_owner_name,
        account_owner.user_email AS account_owner_email,
        account_owner.calendly_link AS account_owner_calendly,
        account_owner.user_photo AS account_owner_photo,
        onboarding.user_photo AS onboarding_specialist_photo,
        onboarding.user_email AS onboarding_specialist_email,
        onboarding.user_full_name AS onboarding_specialist_name,
        deliverability.user_email AS account_deliverability_consultant_email,
        deliverability.user_full_name AS account_deliverability_consultant,
        --opp_source_xf.is_closed,
        contract_source_xf.end_date,
        CASE 
            WHEN base.annual_revenue <= 49999999 THEN 'SMB'
            WHEN base.annual_revenue > 49999999 AND base.annual_revenue <= 499999999 THEN 'Mid-Market'
            WHEN base.annual_revenue > 499999999 THEN 'Enterprise'
        END AS company_size_rev,
        -- COUNT(DISTINCT opp_source_xf.opportunity_id) AS number_of_open_opportunities,
        -- CASE 
        --     WHEN opp_source_xf.is_closed = false THEN COUNT(DISTINCT opp_source_xf.opportunity_id)
        --     ELSE 0
        -- END AS number_of_open_opps,
        CASE
            WHEN base.billing_country IS NOT null AND base.billing_country IN ('GB','UK','IE','DE','DK','FI','IS','NO','SE','FR','AL','AD','AM','AT','BY','BE','BA','BG','HR','CS','CY','CZ','EE','FX','GE','GR','HU','IT','LV','LI','LT','LU','MK','MT','MD','MC','ME','NL','PL','PT','RO','SM','RS','SJ','SK','SI','ES','CH','UA','VA','FO','GI','GG','IM','JE','XK','RU') THEN 'EUROPE'
            WHEN base.billing_country IS NOT null AND base.billing_country IN ('JP','KR','CN','MN','TW','VN','HK','LA','TH','KH','PH','MY','SG','ID','LK','IN','NP','BT','MM','PK','AF','KG','UZ','TM','KZ') THEN 'APJ'
            WHEN base.billing_country IS NOT null AND base.billing_country IN ('AU','CX','NZ','NF','Australia','New Zealand') THEN 'AUNZ'
            WHEN base.billing_country IS NOT null AND base.billing_country IN ('AR','BO','BR','BZ','CL','CO','CU','CR','DO','EC','FK','GF','GS','GY','GT','HN','MX','NI','PA','PE','PR','PY','SR','SV','UY','VE')THEN 'LATAM'
            WHEN base.billing_state IS NOT null AND base.billing_state IN ('CA','NV','UT','AK','MO','CO','HI','OK','IL','AR','NE','MI','KS','OR','WA','ID','WI','MN','ND','SD','MT','WY','IA','NB','ON','PE','QC','AB','BC','MB','SK','NL','NS','YT','NU','NT') THEN 'NA-WEST'
            WHEN base.billing_state IS NOT null AND base.billing_state IN ('NY','CT','MA','VT','NH','ME','NJ','RI','TX','AZ','NM','MS','LA','AL','TN','KY','OH','IN','GA','FL','NC','SC','PA','DC','DE','MD','VA','WV') THEN 'NA-EAST'
            WHEN base.billing_country IS NOT null AND base.billing_country IN ('AG','AI','AN','AW','BB','BM','BS','DM','GD','GP','HT','JM','KN','LC','MQ','MS','TC','TT','VC','VG','VI') THEN 'NA-EAST'
            WHEN base.billing_country IS NOT null AND base.billing_country IN ('US','CA') AND base.billing_state IS null  THEN 'NA-NO-STATEPROV'
            WHEN base.billing_country IS NOT null AND base.billing_state IS NOT null THEN 'ROW'
            ELSE 'Unknown'
        END AS global_region,
        contract_source_xf.end_date+1 AS renewal_date
        -- "Renewal_Notice_Date__c" AS renewal_notice_date,
    FROM base
    LEFT JOIN "acton".salesforce."account" AS parent ON
    base.id=parent.parent_id
    LEFT JOIN {{ref('user_source_xf')}} AS sdr ON
    base.sdr_c=sdr.user_id
    LEFT JOIN {{ref('user_source_xf')}} AS csm ON
    base.csm_c=csm.user_id
    LEFT JOIN {{ref('user_source_xf')}} AS account_owner ON
    base.owner_id=account_owner.user_id
    LEFT JOIN {{ref('user_source_xf')}} AS onboarding ON
    base.onboarding_specialist_c=onboarding.user_id
    LEFT JOIN {{ref('user_source_xf')}} AS deliverability ON
    base.deliverability_consultant_c=deliverability.user_id
    LEFT JOIN {{ref('contract_source_xf')}} ON
    base.current_contract_c=contract_source_xf.contract_id
    WHERE base.is_deleted = 'False'
   -- {{dbt_utils.group_by(n=66) }}


)

SELECT 
final.*,
CASE
    WHEN global_region IN ('EUROPE','ROW','AUNZ') THEN 'EMEA'
    WHEN company_size_rev IN ('SMB') OR company_size_rev IS null THEN 'Velocity'
    WHEN company_size_rev IN ('Mid-Market','Enterprise') THEN 'Upmarket'
    ELSE null
END AS segment 
FROM final