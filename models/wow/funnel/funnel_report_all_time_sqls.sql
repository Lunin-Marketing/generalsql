{{ config(materialized='table') }}

WITH base AS (

    SELECT DISTINCT
        sql_source_xf.sql_id,
        CONCAT('https://acton.my.salesforce.com/',sql_source_xf.sql_id) AS sql_url,
        sql_source_xf.created_date AS sql_date,
        company_size_rev,
        account_global_region,
        opp_lead_source,
        segment,
        industry,
        channel_bucket
    FROM {{ref('sql_source_xf')}}
    WHERE type = 'New Business'

)

SELECT *
FROM base