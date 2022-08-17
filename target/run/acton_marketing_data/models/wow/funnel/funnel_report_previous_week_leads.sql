

  create  table "acton"."dbt_actonmarketing"."funnel_report_previous_week_leads__dbt_tmp"
  as (
    

WITH previous_week AS (

    SELECT
        week 
    FROM "acton"."dbt_actonmarketing"."date_base_xf"
    WHERE day=CURRENT_DATE-14

), base AS (

    SELECT DISTINCT
        person_source_xf.person_id AS lead_id,
        person_source_xf.marketing_created_date AS created_date,
        country,
        global_region
    FROM "acton"."dbt_actonmarketing"."person_source_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."date_base_xf" ON
    person_source_xf.marketing_created_date=date_base_xf.day
    LEFT JOIN previous_week ON 
    date_base_xf.week=previous_week.week
    WHERE previous_week.week IS NOT null

)

SELECT *
FROM base
  );