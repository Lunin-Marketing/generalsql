{{ config(materialized='table') }}
SELECT
day::date AS day,
week,
month,
month_name,
quarter,
fy
FROM {{ source('public', 'date_base') }}