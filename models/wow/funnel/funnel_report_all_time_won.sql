{{ config(materialized='table') }}

WITH base AS (

    SELECT DISTINCT
        opp_sales_source_xf.opportunity_id AS won_id,
        opp_sales_source_xf.close_date AS won_date,
        acv,
        company_size_rev,
        account_global_region
    FROM {{ref('opp_sales_source_xf')}}
    WHERE type = 'New Business'

)

SELECT *
FROM base