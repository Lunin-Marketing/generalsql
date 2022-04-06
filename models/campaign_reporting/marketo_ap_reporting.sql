{{ config(materialized='table') }}

SELECT *
FROM {{ref('new_business_acv_influenced_by_aps_FY21_xf')}}
WHERE LOWER(automated_program_name) LIKE 'marketo'
