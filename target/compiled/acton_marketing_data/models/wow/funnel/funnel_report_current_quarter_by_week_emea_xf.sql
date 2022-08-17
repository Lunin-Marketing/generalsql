

WITH base AS (

    SELECT *
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_quarter_by_week_xf"
    WHERE global_region IN ('EUROPE','APJ','AUNZ','ROW')

)

SELECT
    week,
    SUM(leads) AS leads,
    SUM(mqls) AS mqls,
    SUM(sals) AS sals,
    SUM(sqls) AS sqls,
    SUM(sqos) AS sqos
FROM base
GROUP BY 1