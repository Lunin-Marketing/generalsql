{{ config(materialized='table') }}

SELECT *
FROM {{ref('sfdc_lead_snapshot')}}
ORDER BY 1 DESC
