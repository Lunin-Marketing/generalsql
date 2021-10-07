{{ config(materialized='table') }}
WITH base AS (
SELECT *
FROM "acton".public."Act_On_Instance_User__c"

), final AS (
    SELECT
    "Id" AS ao_user_id,
    "Name" AS ao_user_name,
    "CreatedDate" AS ao_user_created_date,
    "Act_On_Instance__c" AS ao_user_instance,
    "Email__c" AS ao_user_email, 
    "First_Name__c" AS ao_user_first_name, 
    "Last_Name__c" AS ao_user_last_name, 
    "Is_Admin_User__c" AS is_admin_user,
    "Is_Marketing_User__c" AS is_marketing_user,
    "Is_Sales_User__c" AS is_sales_user,
    "Date_User_Created__c" AS ao_user_date_created,
    "AO_User_Account_ID__c" AS ao_user_account_id, 
    "Contact__c" AS ao_user_contact_id,
    "AO_Account_Name__c" AS ao_user_account_name
    FROM base
)
SELECT
*
FROM final