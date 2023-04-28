{{ config(materialized='table') }}

SELECT
    -- Key ID
    lead_source_xf.lead_id AS person_id,

    -- Firmographic Information
    lead_source_xf.email,
    lead_source_xf.first_name,
    lead_source_xf.last_name,
    lead_source_xf.title,
    lead_source_xf.country,
    lead_source_xf.lead_status AS person_status,
    lead_source_xf.global_region,
    lead_source_xf.created_by_id,
    lead_source_xf.form_consent_opt_in,
    lead_source_xf.employee_count,
    lead_source_xf.department,


    -- Account Information
    lead_source_xf.company,
    lead_source_xf.annual_revenue,
    lead_source_xf.lean_data_account_id,
    lead_source_xf.current_crm,
    lead_source_xf.current_ma,
    lead_source_xf.company_size_rev,
    lead_source_xf.industry,
    lead_source_xf.industry_bucket,
    lead_source_xf.segment,
    lead_source_xf.is_current_customer,
    account_source_xf.target_account,
    account_source_xf.account_owner_name,

    -- DateTime Fields
    lead_source_xf.last_modified_date,
    lead_source_xf.created_date,
    lead_source_xf.mql_created_date,
    lead_source_xf.mql_most_recent_date,
    lead_source_xf.working_date,
    lead_source_xf.marketing_created_date,
    lead_source_xf.email_bounced_date_new,
    lead_source_xf.hand_raiser_date,

    -- Other Key Information    
    lead_source_xf.lead_source,    
    lead_source_xf.lead_owner_id AS person_owner_id,
    user_source_xf.user_name AS person_owner_name,
    lead_source_xf.lead_score,
    lead_source_xf.firmographic_demographic_lead_score,
    lead_source_xf.no_longer_with_company,
    CASE
        WHEN lead_source_xf.is_hand_raiser = true THEN 'Yes'
        ELSE 'No'
    END AS is_hand_raiser,
    lead_source_xf.created_by_name,
    lead_source_xf.email_bounced_reason_new,
    lead_source_xf.most_recent_salesloft_cadence,
    lead_source_xf.looking_for_ma,

    --Attribution Data
    lead_source_xf.campaign_first_touch,
    lead_source_xf.campaign_last_touch,
    lead_source_xf.campaign_lead_creation,
    lead_source_xf.channel_first_touch,
    lead_source_xf.channel_last_touch,
    lead_source_xf.channel_lead_creation,
    lead_source_xf.medium_first_touch,
    lead_source_xf.medium_last_touch,
    lead_source_xf.medium_lead_creation,
    lead_source_xf.source_first_touch,
    lead_source_xf.source_last_touch, 
    lead_source_xf.source_lead_creation,    
    lead_source_xf.subchannel_first_touch,
    lead_source_xf.subchannel_last_touch,
    lead_source_xf.subchannel_lead_creation,
    lead_source_xf.offer_asset_name_first_touch,
    lead_source_xf.offer_asset_name_last_touch,
    lead_source_xf.offer_asset_name_lead_creation,
    lead_source_xf.offer_asset_subtype_first_touch,
    lead_source_xf.offer_asset_subtype_last_touch,
    lead_source_xf.offer_asset_subtype_lead_creation,
    lead_source_xf.offer_asset_topic_first_touch,
    lead_source_xf.offer_asset_topic_last_touch,
    lead_source_xf.offer_asset_topic_lead_creation,
    lead_source_xf.offer_asset_type_first_touch,
    lead_source_xf.offer_asset_type_last_touch,
    lead_source_xf.offer_asset_type_lead_creation,
    lead_source_xf.channel_bucket,
    channel_bucket_lt,
    channel_bucket_details
FROM {{ref('lead_source_xf')}}
LEFT JOIN {{ref('account_source_xf')}} ON
lead_source_xf.lean_data_account_id=account_source_xf.account_id
LEFT JOIN {{ref('user_source_xf')}} ON
lead_owner_id=user_id
WHERE is_converted = FALSE
AND lead_source_xf.is_deleted = FALSE
UNION ALL
SELECT
    -- Key ID
    contact_id AS person_id,

    -- Firmographic Information
    email,
    first_name,
    last_name,
    title,
    mailing_country,
    contact_status AS person_status,
    global_region,
    created_by_id,
    form_consent_opt_in,
    employee_count,
    department,

    -- Account Information
    account_name,
    annual_revenue,
    account_id,
    de_current_crm,
    de_current_ma,
    company_size_rev,
    industry,
    industry_bucket,
    segment,
    is_current_customer,
    target_account,
    account_owner_name,
    
    -- DateTime Fields
    last_modified_date,
    created_date,
    mql_created_date,
    mql_most_recent_date,
    working_date,
    marketing_created_date,
    email_bounced_date_new,
    hand_raiser_date,

    -- Other Key Information    
    lead_source,    
    contact_owner_id AS person_owner_id,
    user_source_xf.user_name AS person_owner_name,
    lead_score,
    firmographic_demographic_lead_score,
    is_no_longer_with_company,
    CASE
        WHEN is_hand_raiser = true THEN 'Yes'
        ELSE 'No'
    END AS is_hand_raiser,
    created_by_name,
    email_bounced_reason_new,
    most_recent_salesloft_cadence,
    looking_for_ma,

    --Attribution Data
    campaign_first_touch,
    campaign_last_touch,
    campaign_lead_creation,
    channel_first_touch,
    channel_last_touch,
    channel_lead_creation,
    medium_first_touch,
    medium_last_touch,
    medium_lead_creation,
    source_first_touch,
    source_last_touch, 
    source_lead_creation,    
    subchannel_first_touch,
    subchannel_last_touch,
    subchannel_lead_creation,
    offer_asset_name_first_touch,
    offer_asset_name_last_touch,
    offer_asset_name_lead_creation,
    offer_asset_subtype_first_touch,
    offer_asset_subtype_last_touch,
    offer_asset_subtype_lead_creation,
    offer_asset_topic_first_touch,
    offer_asset_topic_last_touch,
    offer_asset_topic_lead_creation,
    offer_asset_type_first_touch,
    offer_asset_type_last_touch,
    offer_asset_type_lead_creation,
    channel_bucket,
    channel_bucket_lt,
    channel_bucket_details
FROM {{ref('contact_source_xf')}}
LEFT JOIN {{ref('user_source_xf')}} ON
contact_owner_id=user_id
WHERE is_deleted = FALSE