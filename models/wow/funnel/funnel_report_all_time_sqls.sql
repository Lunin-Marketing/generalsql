{{ config(materialized='table') }}

WITH base AS (

    SELECT DISTINCT
        sql_source_xf.sql_id,
        sql_source_xf.created_date AS sql_date,
        company_size_rev,
        account_global_region
    FROM {{ref('sql_source_xf')}}
    WHERE type = 'New Business'

)

SELECT *
FROM base