

  create  table "acton"."dbt_actonmarketing"."kpi_targets__dbt_tmp"
  as (
    

WITH base AS (

SELECT *
FROM "acton"."public"."kpi_targets"

)

SELECT
    kpi_month::Date AS kpi_month,
    kpi_lead_source,
    kpi,
    kpi_target
FROM base
  );