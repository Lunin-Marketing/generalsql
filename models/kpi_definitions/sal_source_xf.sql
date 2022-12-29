{{ config(materialized='table') }}

SELECT
    person_id,
    email,
    is_hand_raiser,
    is_current_customer,
    working_date,
    mql_most_recent_date,
    person_status,
    person_owner_name,
    country,
    global_region,
    company,
    company_size_rev,
    lead_source,
    segment,
    industry,
    industry_bucket,
    target_account,
    channel_bucket,
    channel_bucket_details,
    offer_asset_name_lead_creation,
    most_recent_salesloft_cadence,
    campaign_lead_creation
FROM {{ref('person_source_xf')}}
WHERE person_owner_id != '00Ga0000003Nugr'
AND working_date IS NOT null
AND email NOT LIKE '%act-on.com'
--AND lead_source = 'Marketing'
AND person_status  NOT IN ('Current Customer','Partner','Bad Data','No Fit')