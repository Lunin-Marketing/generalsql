{{ config(materialized='table') }}
WITH base AS (
SELECT *
FROM "acton".salesforce."account"

), final AS (
SELECT
id AS account_id,
is_deleted,
name AS account_name,
type AS account_type,
parent_id AS account_parent_id,
billing_state,
billing_postal_code,
billing_country,
industry,
annual_revenue,
number_of_employees,
owner_id account_owner_id,
created_date AS account_created_date,
last_modified_date,
system_modstamp AS systemmodstamp,
is_partner,
current_customer_c AS is_current_customer,
csm_c AS csm,
current_crm_c AS current_crm,
current_marketing_automation_c AS current_ma,
de_country_c AS de_country,
de_current_crm_c AS de_current_crm,
de_current_marketing_automation_c AS de_current_ma,
de_industry_c AS de_industry,
de_parent_company_c AS de_account_parent_name,
de_ultimate_parent_company_c AS de_ultimate_parent_account_name,
do_not_market_c AS do_not_market,
target_account_c AS target_account,
parent.name AS account_parent_name
--"SF_Id_18_Char__c" AS "18_digit_account_id",
--"SDR_Email__c" AS sdr_email,
-- "SDR__c" AS sdr,
--"Customer_Since__c" AS customer_since,
--"Number_of_Open_Opportunities__c" AS number_of_open_opportunities,
-- "Market_Segment_Static__c" AS market_segment_static,
CASE WHEN annual_revenue <= 49999999 THEN 'SMB'
     WHEN annual_revenue > 49999999 AND "AnnualRevenue" <= 499999999 THEN 'Mid-Market'
     WHEN annual_revenue > 499999999 THEN 'Enterprise'
     END AS company_size_rev,
-- "Account_CSM__c" AS account_csm,
-- "Renewal_Notice_Date__c" AS renewal_notice_date,
-- "Number_of_Open_Opps__c" AS number_of_open_opps,
-- "Current_Contract__c" AS current_contract,
"CSM_Team__c" AS csm_team,
-- "Account_Owner_Email__c" AS account_owner_email, - join to user on owner_id
--"SDR_Photo__c" AS sdr_photo,
ao_instance_number_c AS ao_instance_number,
--"SDR_Calendly__c" AS sdr_calendly,
--"SDR_Title__c" AS sdr_title,
-- "Onboarding_Specialist__c" AS onboarding_specialist,
-- "SDR_First_Name__c" AS sdr_first_name,
-- "Account_Owner_Calendly__c" AS account_owner_calendly,
-- "Onboarding_Completion_Date__c" AS onboarding_completion_date,
-- "Onboarding_Specialist_Email__c" AS onboarding_specialist_email,
-- "Onboarding_Specialist_Photo__c" AS onboarding_specialist_photo,
-- "Account_CSM_Email__c" AS account_csm_email,
-- "Account_CSM_Photo__c" AS account_csm_photo,
-- "SDR_Phone__c" AS sdr_phone,
-- "Account_Owner_Photo__c" AS account_owner_photo,
FROM base
LEFT JOIN "acton".salesforce."account" AS parent ON
account.id=parent.parent_id
)

SELECT 
* 
FROM final