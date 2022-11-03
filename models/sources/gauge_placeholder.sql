{{ config(materialized='table') }}
SELECT *
FROM {{ source('public', 'gauge_placeholder') }}