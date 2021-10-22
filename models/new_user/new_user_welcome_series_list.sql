WITH opp_and_acct_base AS (
SELECT
opportunity_id,
opp_source_xf.account_id,
opportunity_name,
customer_since,
account_csm,
account_csm_email,
account_csm_photo,
onboarding_specialist,
onboarding_specialist_email,
onboarding_specialist_photo,
user_name AS account_owner,
account_owner_email,
account_owner_photo
FROM "acton".dbt_actonmarketing.opp_source_xf
LEFT JOIN "acton".dbt_actonmarketing.account_source_xf ON
opp_source_xf.account_id=account_source_xf.account_id
LEFT JOIN "acton".dbt_actonmarketing.user_source_xf ON
account_source_xf.account_owner_id=user_source_xf.user_id
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
CASE WHEN opp_and_acct_base.account_csm IS null THEN 'Support'
ELSE opp_and_acct_base.account_csm END AS account_csm,
CASE WHEN opp_and_acct_base.account_csm_email IS null THEN 'support@act-on.com'
ELSE opp_and_acct_base.account_csm_email END AS account_csm_email,
CASE WHEN opp_and_acct_base.account_csm_photo IS null THEN 'https://success.act-on.com/cdnr/forpcid1/acton/attachment/9883/f-fa8432de-9cea-4bf7-b6d4-eca1c9656b82/1/-/-/-/-/NewUserWelcomeSeries-EM3-Support.png'
ELSE opp_and_acct_base.account_csm_photo END AS account_csm_photo,
opp_and_acct_base.onboarding_specialist,
opp_and_acct_base.onboarding_specialist_email,
opp_and_acct_base.onboarding_specialist_photo,
opp_and_acct_base.account_owner,
opp_and_acct_base.account_owner_email,
opp_and_acct_base.account_owner_photo,
contact_source_xf.first_name,
contact_source_xf.last_name,
contact_source_xf.email,
ao_instance_user_source_xf.ao_user_id,
ao_instance_user_source_xf.is_marketing_user
FROM opp_and_acct_base
LEFT JOIN "acton".dbt_actonmarketing.contact_source_xf ON
opp_and_acct_base.account_id=contact_source_xf.account_id
LEFT JOIN "acton".dbt_actonmarketing.ao_instance_user_source_xf ON
contact_source_xf.contact_id=ao_instance_user_source_xf.ao_user_contact_id
WHERE 1=1
AND is_renewal_contact = 'true'
AND is_marketing_user = 'true'
