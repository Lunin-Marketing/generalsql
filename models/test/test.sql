{{ config(materialized='table') }}

SELECT
    outreach_prospect_sequence_xf.or_prospect_id,
    or_sequence_tag,
    person_id
FROM {{ref('outreach_prospect_sequence_xf')}}
LEFT JOIN {{ref('person_source_xf')}}
    ON outreach_prospect_sequence_xf.or_prospect_email=person_source_xf.email
WHERE or_sequence_id = '879'