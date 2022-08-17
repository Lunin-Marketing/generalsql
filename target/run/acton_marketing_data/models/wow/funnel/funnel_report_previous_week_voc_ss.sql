

  create  table "acton"."dbt_actonmarketing"."funnel_report_previous_week_voc_ss__dbt_tmp"
  as (
    

WITH previous_week AS (

    SELECT
        week 
    FROM "acton"."dbt_actonmarketing"."date_base_xf"
    WHERE day=CURRENT_DATE-14

), base AS (

    SELECT DISTINCT
        opp_voc_source_ss_xf.opportunity_id AS voc_id,
        acv,
        opp_voc_source_ss_xf.negotiation_date AS sqo_date,
        country,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."opp_voc_source_ss_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."date_base_xf" ON
    opp_voc_source_ss_xf.negotiation_date=date_base_xf.day
    LEFT JOIN previous_week ON 
    date_base_xf.week=previous_week.week
    WHERE previous_week.week IS NOT null
    AND type = 'New Business'

)

SELECT *
FROM base
  );