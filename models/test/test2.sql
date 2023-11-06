{{ config(materialized='table') }}

WITH base AS (

    SELECT DISTINCT
        id,
        date_time_to_working_c,
        channel_last_touch,
        medium_last_touch,
        source_last_touch,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY date_time_to_working_c,dbt_valid_from DESC ) AS mql_row
    FROM {{ref('sfdc_contact_snapshots')}}
    WHERE date_time_to_working_c >= '2023-01-01'

)

SELECT
    id,
    date_time_to_working_c,
    channel_last_touch,
    medium_last_touch,
    source_last_touch
FROM base
WHERE mql_row = 1