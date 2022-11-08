{{ config(materialized='table') }}

SELECT DISTINCT
    opportunity_id,
    opp_channel_lead_creation,
    opp_medium_lead_creation,
    opp_source_lead_creation
FROM {{ref('opp_source_xf')}}
WHERE opp_channel_lead_creation IN ('parnter','syndication partner','tradeshow')
OR opp_medium_lead_creation = 'test'
