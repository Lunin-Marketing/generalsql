{{ config(materialized='table') }}

WITH person_base AS (

    SELECT
        contact_id AS person_id,
        email,
        is_hand_raiser,
        mql_created_date,
        -- owner_id,
        channel_lead_creation,
        medium_lead_creation,
        source_lead_creation,
        lead_source,
        marketing_created_date,
        contact_status AS person_status,
        company_size_rev,
        account_id,
        channel_bucket
    FROM {{ref('contact_source_xf')}}
    UNION ALL
    SELECT
        lead_id AS person_id,
        email,
        is_hand_raiser,
        mql_created_date,
        -- owner_id,
        channel_lead_creation,
        medium_lead_creation,
        source_lead_creation,
        lead_source,
        marketing_created_date,
        lead_status AS person_status,
        company_size_rev,
        person_account_id,
        channel_bucket
    FROM {{ref('lead_source_xf')}}
    WHERE is_converted = false
)

SELECT 
    person_base.person_id,
    person_base.email,
    person_base.is_hand_raiser,
    person_base.channel_bucket,
    person_base.mql_created_date,
    --person_base.owner_id,
    person_base.channel_lead_creation,
    person_base.medium_lead_creation,
    person_base.source_lead_creation,
    person_base.lead_source,
    person_base.marketing_created_date,
    person_base.company_size_rev,
    person_base.account_id,
    person_base.person_status,
    account_base.is_current_customer, 
    opp_base.opportunity_id,
    opp_base.close_date,
    opp_base.is_won,
    opp_base.created_date AS opp_created_date,
    opp_base.discovery_date,
    opp_base.stage_name,
    opp_base.acv,
    opp_base.opp_lead_source,
    opp_base.type,
    opp_base.is_closed
FROM person_base
LEFT JOIN {{ref('opp_source_xf')}} AS opp_base ON
person_base.account_id=opp_base.account_id
LEFT JOIN {{ref('account_source_xf')}} AS account_base ON
person_base.account_id=account_base.account_id
WHERE opportunity_id IS NOT null
ORDER BY 4