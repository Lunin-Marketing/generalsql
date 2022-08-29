{{ config(materialized='table') }}

WITH base AS (

SELECT *
FROM "acton".salesforce."campaign_member"

), final AS (

    SELECT
        id AS campaign_member_id,
        campaign_id,
        lead_id,
        contact_id,
        lead_or_contact_id,
        account_id,
        company_or_account AS account_name,
        account_lookup_c AS account_lookup,
        email,
        status AS campaign_member_status,
        type AS campaign_member_type,
        has_responded AS campaign_member_has_responded,
        DATE_TRUNC('day',base.created_date)::Date AS campaign_member_created_date,
        DATE_TRUNC('day',base.first_responded_date)::Date AS campaign_member_first_responded_date,
        is_deleted AS is_deleted_campaign_member
    FROM base

)

SELECT *
FROM final
    