

  create  table "acton"."dbt_actonmarketing"."funnel_report_rolling3mo_voc__dbt_tmp"
  as (
    

WITH rolling_3mo AS (

    SELECT DISTINCT
        week
    FROM "acton"."dbt_actonmarketing"."date_base_xf"
    WHERE day >= CURRENT_DATE-90
    AND day <= CURRENT_DATE
    ORDER BY 1

), base AS (

    SELECT DISTINCT
        opp_voc_source_xf.opportunity_id AS voc_id,
        opp_voc_source_xf.negotiation_date AS voc_date,
        rolling_3mo.week,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."opp_voc_source_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."date_base_xf" ON
    opp_voc_source_xf.negotiation_date=date_base_xf.day
    LEFT JOIN rolling_3mo ON 
    date_base_xf.week=rolling_3mo.week
    WHERE rolling_3mo.week IS NOT null

)

SELECT *
FROM base
  );