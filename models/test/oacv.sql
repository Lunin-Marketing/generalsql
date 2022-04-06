{{ config(materialized='table') }}

WITH opp_and_acct_base AS (

    SELECT
        opportunity_id,
        opp_source_xf.account_id,
        opportunity_name
    FROM {{ref('opp_source_xf')}}
    LEFT JOIN {{ref('account_source_xf')}} ON
    opp_source_xf.account_id=account_source_xf.account_id
    WHERE stage_name = 'Implement'
    AND is_current_customer = 'true'
    AND customer_since BETWEEN CURRENT_DATE-60 and CURRENT_DATE

)

SELECT DISTINCT
    opp_and_acct_base.opportunity_id,
    opp_and_acct_base.account_id,
    contact_source_xf.email,
    contact_source_xf.contact_id
FROM opp_and_acct_base
LEFT JOIN {{ref('contact_source_xf')}} ON
opp_and_acct_base.account_id=contact_source_xf.account_id