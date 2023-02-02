{{ config(materialized='table') }}

WITH base AS (

    SELECT *
    FROM {{source('outreach_sf','prospect')}}

), prospect_tag AS (

    SELECT *
    FROM {{source('outreach_sf','prospect_tag')}} 

), prospect_email AS (

    SELECT *
    FROM {{source('outreach_sf','prospect_email')}} 

)

SELECT
    --IDs
    base.id AS or_prospect_id,
    relationship_account_id AS or_prospect_account_id,
    relationship_creator_id AS or_prospect_creator_id,
    relationship_owner_id AS or_prospect_owner_id,
    relationship_persona_id AS or_prospect_persona_id,
    relationship_stage_id AS or_prospect_stage_id,
    relationship_updater_id AS or_prospect_updater_id,
    external_id AS external_id,
    linked_in_id AS linked_in_id,
    updater_id AS updater_id,
    stack_overflow_id AS stack_overflow_id,
    sharing_team_id AS sharing_team_id,

    --Prospect Data
    prospect_email.email AS or_prospect_email,
    tag_name AS or_prospect_tag,
    address_city AS or_prospect_city,
    address_country AS or_prospect_country,
    custom_8 AS or_prospect_customer_refrence,
    engaged_score AS or_prospect_engaged_score,
    first_name AS or_prospect_first_name,
    gender AS or_prospect_gender,
    custom_16 AS or_prospect_global_region,
    last_name AS or_prospect_last_name,
    linked_in_employees AS or_prospect_linkedin_employee_count,
    middle_name AS or_prospect_middle_name,
    name AS or_prospect_name,
    nickname AS or_prospect_nickname,
    occupation AS or_prospect_occupation,
    address_zip AS or_prospect_postal_code,
    region AS or_prospect_region,
    score AS or_prospect_score,
    source AS or_prospect_source,
    address_state AS or_prospect_state,
    address_street_2 AS or_prospect_steet_2,
    address_street AS or_prospect_street_1,
    title AS or_prospect_title,
    custom_14 AS or_prospect_vertical,
    custom_15 AS or_prospect_what_they_do,
    custom_1 AS or_prospect_status_reason,
    date_of_birth AS date_of_birth,
    angel_list_url AS angel_list_url,
    calls_opt_status AS calls_opt_status,
    degree AS degree,
    emails_opt_status AS emails_opt_status,
    event_name AS event_name,
    facebook_url AS facebook_url,
    github_url AS github_url,
    github_username AS github_username,
    google_plus_url AS google_plus_url,
    linked_in_slug AS linked_in_slug,
    linked_in_url AS linked_in_url,
    personal_note_1 AS personal_note_1,
    personal_note_2 AS personal_note_2,
    preferred_contact AS preferred_contact,
    quora_url AS quora_url,
    school AS school,
    specialties AS specialties,
    stack_overflow_url AS stack_overflow_url,
    time_zone AS time_zone,
    time_zone_iana AS time_zone_iana,
    time_zone_inferred AS time_zone_inferred,
    twitter_url AS twitter_url,
    twitter_username AS twitter_username,
    website_url_1 AS website_url_1,
    website_url_2 AS website_url_2,
    website_url_3 AS website_url_3,
    click_count AS click_count,
    linked_in_connections AS linked_in_connections,
    email_contacts AS email_contacts,
    sms_opt_status AS sms_opt_status,
    contact_histogram AS contact_histogram,
    persona_name AS persona_name,

    --Prospect Flags
    custom_2 AS is_do_not_contact,
    custom_17 AS is_handraiser,
    custom_3 AS is_no_longer_with_company,
    calls_opted_at AS is_opted_in_calls,
    opted_out AS is_opted_out,
    call_opted_out AS is_opted_out_calls,
    email_opted_out AS is_opted_out_email,
    sms_opted_out AS is_opted_out_sms,

    --Account Data
    account_name AS or_prospect_account_name,
    company AS or_prospect_company,
    company_linked_in_employees AS or_prospect_company_linkedin_employee_count,
    company_linked_in AS company_linked_in,
    company_natural AS company_natural,
    company_type AS company_type,
    company_size AS company_size,
    company_industry AS company_industry,
    company_locality AS company_locality,

    --Activity Data
    custom_4 AS or_prospect_activity_reason,
    campaign_name AS or_prospect_campaign_name,
    custom_5 AS or_prospect_casual_activity_name,
    custom_6 AS or_prospect_common_connect_1,
    custom_9 AS or_prospect_time_of_activity,
    custom_10 AS or_prospect_time_to_connect_1,
    custom_11 AS or_prospect_time_to_connect_2,
    custom_12 AS or_prospect_time_to_connect_3,
    custom_13 AS or_prospect_time_to_connect_4,
    open_count AS open_count,
    reply_count AS reply_count,
    stage_name AS stage_name,

    --Dates
    added_at AS added_at,
    available_at AS available_at,
    base.created_at AS created_at,
    company_founded_at AS company_founded_at,
    emails_opted_at AS emails_opted_at,
    engaged_at AS engaged_at,
    job_start_date AS job_start_date,
    opted_out_at AS opted_out_at,
    touched_at AS touched_at,
    trashed_at AS trashed_at,
    base.updated_at AS updated_at,
    graduation_date AS graduation_date,
    sms_opted_at AS sms_opted_at,

    --OR Data
    base.type AS or_type,
    base.external_owner AS external_owner,
    base.external_source AS external_source,
    base.updater_type AS updater_type,
    custom_35 AS custom_35
FROM base
LEFT JOIN prospect_tag
    ON base.id=prospect_tag.prospect_id
LEFT JOIN prospect_email
    ON base.id=prospect_email.prospect_id