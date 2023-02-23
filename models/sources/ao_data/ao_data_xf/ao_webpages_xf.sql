{{ config(materialized='table') }}

SELECT *
FROM {{ref('ao_webpages')}}
WHERE email IS NOT null