{{ config(materialized='table') }}

SELECT *
FROM {{ref('sfdc_lead_snapshots')}}
ORDER BY 1 DESC
