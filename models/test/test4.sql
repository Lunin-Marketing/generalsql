{{ config(materialized='table') }}

SELECT DISTINCT
    opportunity_id,
    opp_lead_source
FROM {{ref('opp_source_xf')}}
WHERE opp_lead_source NOT IN ('Marketing','SDR','Sales','Channel','CSM','Zendesk')
