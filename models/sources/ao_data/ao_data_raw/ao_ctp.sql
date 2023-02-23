{{ config(materialized='table') }}

WITH base AS (
    SELECT
        "action" AS action,
        "ACTION_DATE" AS action_time,
        "EMAIL" AS email
    FROM {{ source('common', 'fy23_customtouchpoints') }}

)

SELECT
    action,
    action_time::Date AS action_day,
    email
FROM base