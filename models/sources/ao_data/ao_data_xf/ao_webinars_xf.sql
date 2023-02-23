{{ config(materialized='table') }}

SELECT *
FROM {{ref('ao_webinars')}}
WHERE email IS NOT null