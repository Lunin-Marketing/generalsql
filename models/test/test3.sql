{{ config(materialized='table') }}

SELECT  
    opportunity_id,
    opp_channel_lead_creation,
    opp_medium_lead_creation,
    opp_source_lead_creation
FROM opp_source_xf
WHERE channel_bucket = 'Unknown'
AND sql_date >= '2022-10-01'