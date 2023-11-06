{{ config(materialized='table') }}
SELECT
"day"::date AS day,
week,
"month" AS month,
"MONTH_NAME" AS month_name,
"QUARTER" AS quarter,
"FY" AS fy
FROM {{ source('common', 'date_base') }}