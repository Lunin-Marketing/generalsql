{{ config(materialized='table') }}

SELECT
    -- Key ID
    lead_id AS person_id,

    -- Firmographic Information
    email,
    first_name,
    last_name,
    title,
    country,
    lead_status AS person_status,
    global_region,

    -- Account Information
    company,
    annual_revenue,
    lean_data_account_id,
    current_crm,
    current_ma,
    company_size_rev,
    industry,
    industry_bucket,
    segment,

    -- DateTime Fields
    last_modified_date,
    created_date,
    mql_created_date,
    mql_most_recent_date,
    working_date,
    marketing_created_date,

    -- Other Key Information    
    lead_source,    
    lead_owner_id AS person_owner_id,
    lead_score,
    firmographic_demographic_lead_score,
    no_longer_with_company,
    CASE
        WHEN is_hand_raiser = true THEN 'Yes'
        ELSE 'No'
    END AS is_hand_raiser,
    created_by_name,

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
    channel_bucket
FROM {{ref('lead_source_xf')}}
WHERE is_converted = FALSE
AND is_deleted = FALSE
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
    
    -- DateTime Fields
    last_modified_date,
    created_date,
    mql_created_date,
    mql_most_recent_date,
    working_date,
    marketing_created_date,

    -- Other Key Information    
    lead_source,    
    contact_owner_id AS person_owner_id,
    lead_score,
    firmographic_demographic_lead_score,
    is_no_longer_with_company,
    CASE
        WHEN is_hand_raiser = true THEN 'Yes'
        ELSE 'No'
    END AS is_hand_raiser,
    created_by_name,

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
    channel_bucket
FROM {{ref('contact_source_xf')}}
WHERE is_deleted = FALSE