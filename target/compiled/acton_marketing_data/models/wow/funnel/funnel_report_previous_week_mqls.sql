

WITH previous_week AS (

    SELECT
        week
    FROM "acton"."dbt_actonmarketing"."date_base_xf"
    WHERE day=CURRENT_DATE-14

), base AS (

    SELECT DISTINCT
        lead_mql_source_xf.person_id AS mql_id,
        lead_mql_source_xf.mql_created_date AS mql_date,
        country,
        global_region
    FROM "acton"."dbt_actonmarketing"."lead_mql_source_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."date_base_xf" ON
    lead_mql_source_xf.mql_created_date=date_base_xf.day
    LEFT JOIN previous_week ON 
    date_base_xf.week=previous_week.week
    WHERE previous_week.week IS NOT null

)

SELECT *
FROM base