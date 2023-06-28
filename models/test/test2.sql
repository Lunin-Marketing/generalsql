{{ config(materialized='table') }}

SELECT
    lead_id,
    contact_id,
    lead_source_xf.channel_lead_creation AS lead_channel
FROM {{ref('lead_source_xf')}}
LEFT JOIN {{ref('contact_source_xf')}}
    ON lead_source_xf.converted_contact_id=contact_source_xf.contact_id
WHERE is_converted
AND LOWER(contact_source_xf.channel_lead_creation) = 'prospecting'
AND LOWER(lead_source_xf.channel_lead_creation) != 'prospecting'