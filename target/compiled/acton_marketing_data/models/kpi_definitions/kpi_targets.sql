

WITH base AS (

SELECT *
FROM AO_MARKETING.public.kpi_targets

)

SELECT
    kpi_month::Date AS kpi_month,
    kpi_lead_source,
    kpi,
    kpi_target
FROM base