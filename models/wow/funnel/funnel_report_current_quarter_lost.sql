{{ config(materialized='table') }}

WITH current_quarter AS (

    SELECT
        fy,
        quarter 
    FROM {{ref('date_base_xf')}}
    WHERE day=CURRENT_DATE

), base AS (

        SELECT DISTINCT
        opp_source_xf.opportunity_id AS lost_id,
        acv_deal_size_usd,
        week,
        billing_country AS country,
        account_global_region
    FROM {{ref('opp_source_xf')}}
    LEFT JOIN {{ref('date_base_xf')}} ON
    opp_source_xf.close_date=date_base_xf.day
    LEFT JOIN {{ref('account_source_xf')}} ON
    opp_source_xf.account_id=account_source_xf.account_id
    LEFT JOIN current_quarter ON 
    date_base_xf.quarter=current_quarter.quarter
    WHERE current_quarter.quarter IS NOT null
    AND close_date IS NOT null
    AND discovery_date IS NOT null
    AND type = 'New Business'
    AND stage_name IN ('Closed – Lost No Resources / Budget','Closed – Lost Not Ready / No Decision','Closed – Lost Product Deficiency','Closed - Lost to Competitor')

)

SELECT *
FROM base