{{ config(materialized='table') }}

SELECT *
FROM {{ref('ao_emails')}}
WHERE email IS NOT null