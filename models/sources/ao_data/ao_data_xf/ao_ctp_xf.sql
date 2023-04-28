{{ config(materialized='table') }}

SELECT *
FROM {{source('common','fy23_custom_touch_points')}}