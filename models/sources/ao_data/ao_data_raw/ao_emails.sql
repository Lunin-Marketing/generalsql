{{ config(materialized='table') }}

WITH base AS (
    SELECT
        "Action" AS action,
        "Action Time" AS action_time,
        "Recipient E-mail" AS email
    FROM {{ source('common', 'fy23_emailclicks') }}

)

SELECT
    action,
    action_time::Date AS action_day,
    email
FROM base