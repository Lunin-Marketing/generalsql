{{ config(materialized='table') }}

WITH member_base AS (

    SELECT
        person_id,
        campaign_member_first_responded_date
    FROM person_source_xf
    LEFT JOIN campaign_member_source_xf
        ON person_source_xf.person_id=campaign_member_source_xf.lead_or_contact_id
    WHERE campaign_id LIKE '7011O000002bmT7%'
    AND campaign_member_first_responded_date >= '2022-01-01'

), person_history AS (

    SELECT
        lead_id AS person_id,
        field_modified_at,
        old_value,
        new_value
    FROM lead_history_xf
    WHERE field = 'X9883_Lead_Score__c'
    UNION ALL
    SELECT
        contact_id,
        field_modified_at,
        old_value,
        new_value
    FROM contact_history_xf
    WHERE field = 'X9883_Lead_Score__c'

), member_scoring AS (

    SELECT DISTINCT
        member_base.person_id,
        campaign_member_first_responded_date,
        field_modified_at::Date AS field_modified_at,
        old_value,
        new_value
    FROM member_base
    LEFT JOIN person_history
        ON member_base.person_id=person_history.person_id
    WHERE field_modified_at >= campaign_member_first_responded_date

), member_mqls AS (

    SELECT
        person_id,
        campaign_member_first_responded_date,
        field_modified_at,
        old_value,
        new_value
    FROM member_scoring
    WHERE new_value >= '50'
    AND old_value < '50'

), sqos AS (

    SELECT
        member_mqls.person_id,
        campaign_member_first_responded_date,
        field_modified_at,
        old_value,
        new_value,
        opportunity_id,
        sqo_date
    FROM member_mqls
    LEFT JOIN lead_to_cw_cr_cohort
        ON member_mqls.person_id=lead_to_cw_cr_cohort.person_id
    WHERE sqo_date >= campaign_member_first_responded_date
)

SELECT *
FROM member_mqls