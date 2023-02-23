{{ config(materialized='table') }}

SELECT *
FROM {{ref('ao_landingpages')}}
WHERE email IS NOT null