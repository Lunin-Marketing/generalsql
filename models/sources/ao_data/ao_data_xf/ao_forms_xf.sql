{{ config(materialized='table') }}

SELECT
-- IDs
    form_id,
    email,
    unique_visitor_id,
    record_id,

--Form Attributes
    form_title,

--Action Attributes
    action,
    action_time,
    action_day,
    referral_url,

--Other Data
    ip_address,
    cookie_id
FROM {{ref('ao_forms')}}