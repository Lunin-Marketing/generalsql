

  create  table "acton"."dbt_actonmarketing"."funnel_report_current_quarter_won__dbt_tmp"
  as (
    

WITH current_quarter AS (

    SELECT
        fy,
        quarter 
    FROM "acton"."dbt_actonmarketing"."date_base_xf"
    WHERE day=CURRENT_DATE

), base AS (

    SELECT DISTINCT
        opp_sales_source_xf.opportunity_id AS won_id,
        week,
        acv,
        country,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."opp_sales_source_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."date_base_xf" ON
    opp_sales_source_xf.close_date=date_base_xf.day
    LEFT JOIN current_quarter ON 
    date_base_xf.quarter=current_quarter.quarter
    WHERE current_quarter.quarter IS NOT null
    AND type = 'New Business'

)

SELECT *
FROM base
  );