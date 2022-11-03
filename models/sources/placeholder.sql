{{ config(materialized='table') }}
SELECT *
FROM {{ source('public', 'placeholder') }}