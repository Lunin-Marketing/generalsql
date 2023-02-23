{{ config(materialized='table') }}

SELECT DISTINCT
    opportunity_id,
    cr.channel_lead_creation AS cr_channel,
    cr.medium_lead_creation AS cr_medium,
    cr.source_lead_creation AS cr_source
FROM opp_source_xf
LEFT JOIN person_source_xf cr
    ON opp_source_xf.contact_role_contact_id=cr.person_id
WHERE opp_source_xf.channel_bucket = 'Unknown'
AND sql_date >= '2022-10-01'