{{ config(materialized='table') }}

WITH base AS (

    SELECT
        "Action" AS action,
        "Action Time" AS action_time,
        "Response E-mail" AS email
    FROM {{ source('common', 'fy23_formsubmissions') }}

)

SELECT 
    action,
    action_time::Date AS action_day,
    email
FROM base