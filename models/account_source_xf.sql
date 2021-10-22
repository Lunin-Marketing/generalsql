{{ config(materialized='table') }}
WITH base AS (
SELECT *
FROM "acton".public."Account"

), final AS (
SELECT
"Id" AS account_id,
"IsDeleted" AS is_deleted,
"Name" AS account_name,
"Type" AS account_type,
"ParentId" AS account_parent_id,
"BillingState" AS billing_state,
"BillingPostalCode" AS billing_postal_code,
"BillingCountry" AS billing_country,
"NumberOfEmployees" AS number_of_employees,
"Industry" AS industry,
"de_Industry__c" AS de_industry,
"Parent_Name__c" AS account_parent_name,
"de_Parent_Company__c" AS de_account_parent_name,
"de_Ultimate_Parent_Company__c" AS de_ultimate_parent_account_name,
"SF_Id_18_Char__c" AS "18_digit_account_id",
"OwnerId" AS account_owner_id,
"AnnualRevenue" AS annual_revenue,
"de_Current_CRM__c" AS de_current_crm,
"Current_Marketing_Automation__c" AS current_ma,
"de_Current_Marketing_Automation__c" AS de_current_ma,
"SDR_Email__c" AS sdr_email,
"SDR__c" AS sdr,
"Current_Customer__c" AS is_current_customer,
"Customer_Since__c" AS customer_since,
"Do_Not_Market__c" AS do_not_market,
"Number_of_Open_Opportunities__c" AS number_of_open_opportunities,
"Market_Segment_Static__c" AS market_segment_static,
"Market_Segment_Dynamic__c" AS market_segment_dynamic,
"de_Country__c" AS de_country,
"LastModifiedDate" AS last_modified_date,
"CreatedDate" AS account_created_date,
CASE WHEN "AnnualRevenue" <= 49999999 THEN 'SMB'
     WHEN "AnnualRevenue" > 49999999 AND "AnnualRevenue" <= 499999999 THEN 'Mid-Market'
     WHEN "AnnualRevenue" > 499999999 THEN 'Enterprise'
     END AS company_size_rev,
"CSM__c" AS csm,
"Account_CSM__c" AS account_csm,
"Renewal_Notice_Date__c" AS renewal_notice_date,
"Number_of_Open_Opps__c" AS number_of_open_opps,
"Current_Contract__c" AS current_contract,
"CSM_Team__c" AS csm_team,
"Account_Owner_Email__c" AS account_owner_email,
"SDR_Photo__c" AS sdr_photo,
"AO_Instance_Number__c" AS ao_instance_number,
"SDR_Calendly__c" AS sdr_calendly,
"SDR_Title__c" AS sdr_title,
"Onboarding_Specialist__c" AS onboarding_specialist,
"SDR_First_Name__c" AS sdr_first_name,
"Account_Owner_Calendly__c" AS account_owner_calendly,
"Onboarding_Completion_Date__c" AS onboarding_completion_date,
"Onboarding_Specialist_Email__c" AS onboarding_specialist_email,
"Onboarding_Specialist_Photo__c" AS onboarding_specialist_photo,
"Account_CSM_Email__c" AS account_csm_email,
"Account_CSM_Photo__c" AS account_csm_photo,
"SDR_Phone__c" AS sdr_phone,
"Account_Owner_Photo__c" AS account_owner_photo
FROM base
)

SELECT 
* 
FROM final