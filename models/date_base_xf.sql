{{ config(materialized='table') }}
SELECT
day,
week,
month,
quarter,
fy
FROM "defaultdb".public.date_base