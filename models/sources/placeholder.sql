{{ config(materialized='table') }}
SELECT *
FROM {{ source('common', 'placeholder') }}