{{ config(materialized='table') }}
SELECT
day,
week,
month,
quarter,
fy
FROM "acton".public.date_base