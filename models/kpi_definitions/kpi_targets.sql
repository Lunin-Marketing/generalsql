{{ config(materialized='table') }}

WITH base AS (

SELECT *
FROM {{ source('common', 'kpi_targets') }}

)

SELECT
    kpi_month::Date AS kpi_month,
    kpi_lead_source,
    kpi,
    kpi_target
FROM base
ORDER BY 1,3,2