{{ config(materialized='table') }}
SELECT *
FROM {{ source('common', 'gauge_placeholder') }}