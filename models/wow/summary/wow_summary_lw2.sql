{{ config(materialized='table') }}

WITH base AS (

    SELECT *
    FROM {{ref('wow_summary_lw')}}

)

SELECT
    '1.Lead' AS kpi,
    leads AS qty,
    lead_acv AS acv,
    global_region
FROM base
UNION ALL
SELECT
    '2.MQL' AS kpi,
    mqls,
    mql_acv,
    global_region
FROM base
UNION ALL
SELECT
    '3.SAL' AS kpi,
    sals,
    sal_acv,
    global_region
FROM base
UNION ALL
SELECT
    '4.SQL' AS kpi,
    sqls,
    sql_acv,
    global_region
FROM base
UNION ALL
SELECT
    '5.SQO' AS kpi,
    sqos,
    sqo_acv,
    global_region
FROM base
UNION ALL
SELECT
    '6.Demo' AS kpi,
    demo,
    demo_acv,
    global_region
FROM base
UNION ALL
SELECT
    '7.VOC' AS kpi,
    voc,
    voc_acv,
    global_region
FROM base
UNION ALL
SELECT
    '8.Closing' AS kpi,
    closing,
    closing_acv,
    global_region
FROM base
UNION ALL
SELECT
    '9.Pipeline' AS kpi,
    SUM(sqos+demo+voc+closing),
    SUM(sqo_acv+demo_acv+voc_acv+closing_acv),
    global_region
FROM base
GROUP BY 1,4
UNION ALL
SELECT
    '10.Implement' AS kpi,
    won,
    won_acv,
    global_region
FROM base
UNION ALL
SELECT
    '11.Lost' AS kpi,
    lost,
    lost_acv,
    global_region
FROM base