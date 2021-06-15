{{ config(materialized='table') }}
WITH base AS (
SELECT *
FROM "acton".public."Account"
)

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
"OwnerId" AS owner_id,
"AnnualRevenue" AS annual_revenue,
"de_Current_CRM__c" AS current_crm,
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
"CreatedDate" AS account_created_date
FROM base