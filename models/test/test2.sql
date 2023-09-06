{{ config(materialized='table') }}

WITH base AS (

    SELECT DISTINCT
        id,
        date_time_to_working_c,
        lt_utm_channel_c,
        lt_utm_medium_c,
        lt_utm_source_c,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY date_time_to_working_c,dbt_valid_from DESC ) AS mql_row
    FROM {{ref('sfdc_lead_snapshots')}}
    WHERE date_time_to_working_c >= '2023-01-01'

)

SELECT
    id,
    date_time_to_working_c,
    lt_utm_channel_c,
    lt_utm_medium_c,
    lt_utm_source_c
FROM base
WHERE mql_row = 1