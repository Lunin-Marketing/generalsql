

WITH previous_week AS (

    SELECT
        week
    FROM "acton"."dbt_actonmarketing"."date_base_xf"
    WHERE day=CURRENT_DATE-14

), base AS (

    SELECT DISTINCT
        sal_source_xf.person_id AS sal_id,
        sal_source_xf.working_date AS sal_date,
        country,
        global_region
    FROM "acton"."dbt_actonmarketing"."sal_source_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."date_base_xf" ON
    sal_source_xf.working_date=date_base_xf.day
    LEFT JOIN previous_week ON 
    date_base_xf.week=previous_week.week
    WHERE previous_week.week IS NOT null

)

SELECT *
FROM base