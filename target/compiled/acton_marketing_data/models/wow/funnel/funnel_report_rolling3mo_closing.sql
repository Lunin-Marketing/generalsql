

WITH rolling_3mo AS (

    SELECT DISTINCT
        week
    FROM "acton"."dbt_actonmarketing"."date_base_xf"
    WHERE day >= CURRENT_DATE-90
    AND day <= CURRENT_DATE
    ORDER BY 1

), base AS (

    SELECT DISTINCT
        opp_closing_source_xf.opportunity_id AS closing_id,
        opp_closing_source_xf.closing_date AS closing_date,
        --opp_closing_source_xf.closing_date AS closing_date,
        rolling_3mo.week,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."opp_closing_source_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."date_base_xf" ON
    opp_closing_source_xf.closing_date=date_base_xf.day
    --opp_closing_source_xf.closing_date=date_base_xf.day
    LEFT JOIN rolling_3mo ON 
    date_base_xf.week=rolling_3mo.week
    WHERE rolling_3mo.week IS NOT null

)

SELECT *
FROM base