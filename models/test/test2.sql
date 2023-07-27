{{ config(materialized='table') }}

SELECT 
    id
FROM {{ref('sfdc_lead_snapshots')}}
WHERE is_deleted = true
AND 