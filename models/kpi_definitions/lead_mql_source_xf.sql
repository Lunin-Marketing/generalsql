{{ config(materialized='table') }}

SELECT
    person_id,
    email,
    is_hand_raiser,
    is_current_customer,
    mql_created_date,
    mql_most_recent_date,
    working_date,
    last_activity_date,
    person_status,
    record_type,
    country,
    company,
    first_name,
    last_name,
    title,
    person_owner_id,
    person_owner_name,
    global_region,
    company_size_rev,
    annual_revenue,
    lead_source,
    lead_score,
    segment,
    industry,
    target_account,
    industry_bucket,
    channel_bucket,
    channel_bucket_details,
    offer_asset_name_lead_creation,
    most_recent_salesloft_cadence,
    campaign_lead_creation,
    abm_campaign_initial,
    abm_campaign_most_recent,
    abm_date_time_initial,
    abm_date_time_most_recent,
    is_abm
FROM {{ref('person_source_xf')}}
WHERE mql_most_recent_date IS NOT null
AND person_owner_id != '00Ga0000003Nugr' -- AO-Fake Leads
AND email NOT LIKE '%act-on.com'