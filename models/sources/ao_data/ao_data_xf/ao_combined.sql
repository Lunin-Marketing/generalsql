{{ config(materialized='table') }}

SELECT *
FROM {{ref('ao_emails_xf')}}
UNION ALL
SELECT *
FROM {{ref('ao_forms_xf')}}
UNION ALL
SELECT *
FROM {{ref('ao_landingpages_xf')}}
UNION ALL
SELECT *
FROM {{ref('ao_webinars_xf')}}
UNION ALL
SELECT *
FROM {{ref('ao_webpages_xf')}}
UNION ALL 
SELECT *
FROM {{ref('ao_ctp')}}