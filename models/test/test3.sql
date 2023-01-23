{{ config(materialized='table') }}

SELECT
    sql_id,
    opp_channel_lead_creation,
    opp_medium_lead_creation,
    opp_source_lead_creation
FROM sql_source_xf
WHERE channel_bucket = 'Other'