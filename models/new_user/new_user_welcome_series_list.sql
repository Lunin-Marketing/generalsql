WITH opp_and_acct_base AS (
SELECT
opportunity_id,
opp_source_xf.account_id,
opportunity_name,
customer_since
FROM "acton".dbt_actonmarketing.opp_source_xf
LEFT JOIN "acton".dbt_actonmarketing.account_source_xf ON
opp_source_xf.account_id=account_source_xf.account_id
WHERE stage_name = 'Implement'
AND is_current_customer = 'true'
AND customer_since BETWEEN CURRENT_DATE-30 and CURRENT_DATE
AND onboarding_completion_date IS NOT null
)

SELECT 
opp_and_acct_base.opportunity_id,
opp_and_acct_base.account_id,
opp_and_acct_base.opportunity_name,
opp_and_acct_base.customer_since,
email,
ao_user_id,
is_marketing_user
FROM opp_and_acct_base
LEFT JOIN "acton".dbt_actonmarketing.contact_source_xf ON
opp_and_acct_base.account_id=contact_source_xf.account_id
LEFT JOIN "acton".dbt_actonmarketing.ao_instance_user_source_xf ON
contact_source_xf.contact_id=ao_instance_user_source_xf.ao_user_contact_id
WHERE 1=1
AND is_renewal_contact = 'true'
AND is_marketing_user = 'true'
