{{ config(materialized='table') }}

SELECT
    form_response_email,
    {{ dbt_date.convert_timezone("form_date_time", "America/Los_Angeles", "UTC") }}::Date AS form_date_time,
    form_title
FROM {{ref('ao_form')}}
WHERE form_verb IN ('i','u')