{{ config(materialized='table') }}

SELECT *
FROM {{ref('ao_ctp')}}
WHERE email IS NOT null