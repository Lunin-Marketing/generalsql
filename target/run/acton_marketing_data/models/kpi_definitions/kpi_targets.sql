
  
    

        create or replace transient table AO_MARKETING.dbt_snowflake.kpi_targets  as
        (

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
        );
      
  