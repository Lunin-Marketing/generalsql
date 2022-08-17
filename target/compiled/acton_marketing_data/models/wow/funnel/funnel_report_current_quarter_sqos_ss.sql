

WITH current_quarter AS (

    SELECT
        fy,
        quarter 
    FROM "acton"."dbt_actonmarketing"."date_base_xf"
    WHERE day=CURRENT_DATE

), base AS (

    SELECT DISTINCT
        sqo_source_ss_xf.opportunity_id AS sqo_id,
        sqo_source_ss_xf.discovery_date AS sqo_date,
        acv,
        country,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."sqo_source_ss_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."date_base_xf" ON
    sqo_source_ss_xf.discovery_date=date_base_xf.day
    LEFT JOIN current_quarter ON 
    date_base_xf.quarter=current_quarter.quarter
    WHERE current_quarter.quarter IS NOT null
    AND type = 'New Business'

)

SELECT *
FROM base