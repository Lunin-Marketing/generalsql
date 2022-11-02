{{ config(materialized='table') }}

SELECT
    1 AS person_account_id
FROM {{ref('person_source_xf')}}