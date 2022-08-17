

  create  table "acton"."dbt_actonmarketing"."funnel_report_current_week_closing__dbt_tmp"
  as (
    

WITH current_week AS (

    SELECT
        week
    FROM "acton"."dbt_actonmarketing"."date_base_xf"
    WHERE day=CURRENT_DATE-7

), base AS (

    SELECT DISTINCT
        opp_closing_source_xf.opportunity_id AS closing_id,
        acv,
        opp_closing_source_xf.closing_date AS closing_date,
        country,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."opp_closing_source_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."date_base_xf" ON
    opp_closing_source_xf.closing_date=date_base_xf.day
    LEFT JOIN current_week ON 
    date_base_xf.week=current_week.week
    WHERE current_week.week IS NOT null
    AND type = 'New Business'

)

SELECT *
FROM base
  );