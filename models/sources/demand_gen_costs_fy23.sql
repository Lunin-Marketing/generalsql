{{ config(materialized='table') }}

WITH base AS (

    SELECT *
    FROM {{source('common','demand_gen_cost_fy23')}}

)

SELECT
    "CHANNEL" AS channel,
    "MEDIUM" AS medium,
    "source" AS source,
    "CHANNEL_BUCKET" AS channel_bucket,
    "COST" AS cost,
    "START_DATE"::Date AS start_date,
    "END_DATE"::Date AS end_date
FROM base