{{ config(materialized='table') }}

SELECT DISTINCT
    mql_id,
    sqo_id  
FROM {{ref('lead_to_cw_cohort')}}
WHERE mql_created_date >= '2023-01-01'
AND sqo_id IS NOT null