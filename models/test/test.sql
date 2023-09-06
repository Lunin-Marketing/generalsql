{{ config(materialized='table') }}

WITH base AS (

    SELECT DISTINCT
        id,
        mql_most_recent_date_c,
        lt_utm_channel_c,
        lt_utm_medium_c,
        lt_utm_source_c,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY mql_most_recent_date_c,dbt_valid_from DESC ) AS mql_row
    FROM {{ref('sfdc_lead_snapshots')}}
    WHERE mql_most_recent_date_c >= '2023-01-01'
    -- AND id = '00Q5Y00002QjEqeUAF'

)

SELECT
    id,
    mql_most_recent_date_c,
    lt_utm_channel_c,
    lt_utm_medium_c,
    lt_utm_source_c
FROM base
WHERE mql_row = 1