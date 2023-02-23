{{ config(materialized='table') }}

SELECT *
FROM {{ref('ao_forms')}}
WHERE email IS NOT null