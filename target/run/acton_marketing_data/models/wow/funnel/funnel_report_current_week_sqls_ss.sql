

  create  table "acton"."dbt_actonmarketing"."funnel_report_current_week_sqls_ss__dbt_tmp"
  as (
    

WITH current_week AS (

    SELECT
        week
    FROM "acton"."dbt_actonmarketing"."date_base_xf"
    WHERE day=CURRENT_DATE-7

), base AS (

    SELECT DISTINCT
        sql_id,
        sql_source_ss_xf.created_date AS sql_date,
        country,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."sql_source_ss_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."date_base_xf" ON
    sql_source_ss_xf.created_date::Date=date_base_xf.day
    LEFT JOIN current_week ON 
    date_base_xf.week=current_week.week
    WHERE current_week.week IS NOT null
    AND type = 'New Business'

)

SELECT *
FROM base
  );