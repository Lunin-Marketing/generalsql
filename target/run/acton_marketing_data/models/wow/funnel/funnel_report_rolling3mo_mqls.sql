

  create  table "acton"."dbt_actonmarketing"."funnel_report_rolling3mo_mqls__dbt_tmp"
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
        lead_mql_source_xf.person_id AS mql_id,
        lead_mql_source_xf.mql_most_recent_date AS mql_date,
        rolling_3mo.week,
        global_region
    FROM "acton"."dbt_actonmarketing"."lead_mql_source_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."date_base_xf" ON
    lead_mql_source_xf.mql_most_recent_date=date_base_xf.day
    LEFT JOIN rolling_3mo ON 
    date_base_xf.week=rolling_3mo.week
    WHERE rolling_3mo.week IS NOT null

)

SELECT *
FROM base
  );