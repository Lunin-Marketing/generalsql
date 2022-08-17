

WITH current_quarter AS (

    SELECT
        fy,
        quarter 
    FROM "acton"."dbt_actonmarketing"."date_base_xf"
    WHERE day=CURRENT_DATE

), base AS (

    SELECT DISTINCT
        sql_source_xf.sql_id,
        sql_source_xf.created_date AS sql_date,
        week,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."sql_source_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."date_base_xf" ON
    sql_source_xf.created_date=date_base_xf.day
    LEFT JOIN current_quarter ON 
    date_base_xf.quarter=current_quarter.quarter
    WHERE current_quarter.quarter IS NOT null
    AND type = 'New Business'

)

SELECT *
FROM base