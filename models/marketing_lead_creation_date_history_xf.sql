{{ config(materialized='table') }}

WITH base AS (

    SELECT 
        *,
        'lead' AS type
    FROM {{ref('lead_history_xf')}}
    UNION ALL
    SELECT 
        *,
        'contact' AS type
    FROM {{ref('contact_history_xf')}}

)

SELECT
lead_id,
old_value,
new_value,
field_modified_at
FROM base 
WHERE field ='X9883_Lead_Score__c'
AND field_modified_at::Date >= '2022-01-01'
    --OR field_modified_at::Date LIKE '2021%')
ORDER BY lead_id,field_modified_at
