

  create  table "acton"."dbt_actonmarketing"."funnel_report_current_week_sals_ss__dbt_tmp"
  as (
    

WITH current_week AS (

    SELECT
        week
    FROM "acton"."dbt_actonmarketing"."date_base_xf"
    WHERE day=CURRENT_DATE-7

), base AS (

    SELECT DISTINCT
        sal_source_ss_xf.lead_id AS sal_id,
        sal_source_ss_xf.working_date AS sal_date,
        country,
        global_region
    FROM "acton"."dbt_actonmarketing"."sal_source_ss_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."date_base_xf" ON
    sal_source_ss_xf.working_date=date_base_xf.day
    LEFT JOIN current_week ON 
    date_base_xf.week=current_week.week
    WHERE current_week.week IS NOT null

)

SELECT *
FROM base
  );