{{ config(materialized='table') }}
SELECT
day::Date,
week,
month,
month_name,
quarter,
fy
FROM "acton".public.date_base