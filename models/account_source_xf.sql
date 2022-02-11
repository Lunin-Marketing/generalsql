{{ config(materialized='table') }}
WITH base AS (
SELECT *
FROM "acton".salesforce."account"

), final AS (
SELECT
    base.id AS account_id,
    base.is_deleted,
    base.name AS account_name,
    base.type AS account_type,
    base.parent_id AS account_parent_id,
    base.billing_state,
    base.billing_postal_code,
    base.billing_country,
    base.industry,
    base.annual_revenue,
    base.number_of_employees,
    base.owner_id AS account_owner_id,
    base.created_date AS account_created_date,
    base.last_modified_date,
    base.system_modstamp AS systemmodstamp,
    base.is_partner,
    base.current_customer_c AS is_current_customer,
    base.csm_c AS account_csm,
    base.current_crm_c AS current_crm,
    base.current_marketing_automation_c AS current_ma,
    base.de_country_c AS de_country,
    base.de_current_crm_c AS de_current_crm,
    base.de_current_marketing_automation_c AS de_current_ma,
    base.de_industry_c AS de_industry,
    base.de_parent_company_c AS de_account_parent_name,
    base.de_ultimate_parent_company_c AS de_ultimate_parent_account_name,
    base.ao_instance_number_c AS ao_instance_number,
    base.do_not_market_c AS do_not_market,
    base.target_account_c AS target_account,
    base.market_segment_static_c AS market_segment_static,
    base.sdr_c AS sdr,
    base.current_contract_c AS current_contract,
    base.onboarding_completion_date_c AS onboarding_completion_date,
    base.customer_since_c AS customer_since,
    base.onboarding_specialist_c AS onboarding_specialist,
    parent.name AS account_parent_name,
    base.deliverability_consultant_c AS deliverability_consultant_id,
    sdr.user_email AS sdr_email,
    sdr.user_first_name AS sdr_first_name,
    sdr.user_full_name AS sdr_full_name,
    sdr.user_photo AS sdr_photo,
    sdr.calendly_link AS sdr_calendly,
    sdr.user_title AS sdr_title,
    sdr.user_phone AS sdr_phone,
    csm.user_email AS csm_email,
    csm.user_photo AS csm_photo,
    csm.user_id AS csm_id,
    csm.user_full_name AS csm_name,
    account_owner.user_full_name AS account_owner_name,
    account_owner.user_email AS account_owner_email,
    account_owner.calendly_link AS account_owner_calendly,
    account_owner.user_photo AS account_owner_photo,
    onboarding.user_photo AS onboarding_specialist_photo,
    onboarding.user_email AS onboarding_specialist_email,
    onboarding.user_full_name AS onboarding_specialist_name,
    deliverability.user_email AS account_deliverability_consultant_email,
    deliverability.user_full_name AS account_deliverability_consultant,
    opp_source_xf.is_closed,
    CASE 
        WHEN base.annual_revenue <= 49999999 THEN 'SMB'
        WHEN base.annual_revenue > 49999999 AND base.annual_revenue <= 499999999 THEN 'Mid-Market'
        WHEN base.annual_revenue > 499999999 THEN 'Enterprise'
     END AS company_size_rev,
     COUNT(DISTINCT opp_source_xf.opportunity_id) AS number_of_open_opportunities,
     CASE 
        WHEN opp_source_xf.is_closed = false THEN COUNT(DISTINCT opp_source_xf.opportunity_id)
        ELSE 0
    END AS number_of_open_opps

-- "Renewal_Notice_Date__c" AS renewal_notice_date,
FROM base
LEFT JOIN "acton".salesforce."account" AS parent ON
base.id=parent.parent_id
LEFT JOIN {{ref('user_source_xf')}} AS sdr ON
base.sdr_c=sdr.user_id
LEFT JOIN {{ref('user_source_xf')}} AS csm ON
base.csm_c=csm.user_id
LEFT JOIN {{ref('user_source_xf')}} AS account_owner ON
base.owner_id=account_owner.user_id
LEFT JOIN {{ref('user_source_xf')}} AS onboarding ON
base.onboarding_specialist_c=onboarding.user_id
LEFT JOIN {{ref('user_source_xf')}} AS deliverability ON
base.deliverability_consultant_c=deliverability.user_id
LEFT JOIN {{ref('opp_source_xf')}} ON
base.id=opp_source_xf.account_id
{{dbt_utils.group_by(n=58) }}

)

SELECT 
* 
FROM final