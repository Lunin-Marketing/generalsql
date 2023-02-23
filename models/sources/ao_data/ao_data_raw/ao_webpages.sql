{{ config(materialized='table') }}

WITH base AS (
    SELECT
        "Action" AS action,
        "Action Time" AS action_time,
        "Contact E-mail" AS contact_email,
        "EMAIL" AS email,
        "Submitted by Email" AS submitted_by_email
    FROM {{ source('common', 'fy23_webpages') }}

)

SELECT
    action,
    action_time::Date AS action_day,
    COALESCE(contact_email,email,submitted_by_email) AS email
FROM base