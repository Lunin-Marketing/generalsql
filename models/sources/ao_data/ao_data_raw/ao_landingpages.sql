{{ config(materialized='table') }}

WITH base AS (

    SELECT
        "Action" AS action,
        "Action Time" AS action_time,
        "E-mail Address" AS email_address,
        "EMAIL" AS email,
        "Submitted by Email" AS submitted_by_email
    FROM {{ source('common', 'fy23_landingpages') }}

)

SELECT
    action,
    action_time::Date AS action_day,
    COALESCE(email_address,email,submitted_by_email) AS email
FROM base