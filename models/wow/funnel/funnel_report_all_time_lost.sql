{{ config(materialized='table') }}

WITH base AS (

        SELECT DISTINCT
        opp_source_xf.opportunity_id AS lost_id,
        acv_deal_size_usd,
        opp_source_xf.close_date AS close_date,
        account_global_region,
        company_size_rev
    FROM {{ref('opp_source_xf')}}
    LEFT JOIN {{ref('account_source_xf')}} ON
    opp_source_xf.account_id=account_source_xf.account_id
    AND close_date IS NOT null
    AND discovery_date IS NOT null
    AND type = 'New Business'
    AND stage_name IN ('Closed – Lost No Resources / Budget','Closed – Lost Not Ready / No Decision','Closed – Lost Product Deficiency','Closed - Lost to Competitor')

)

SELECT *
FROM base