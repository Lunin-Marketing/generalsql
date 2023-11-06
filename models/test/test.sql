{{ config(materialized='table') }}

WITH base AS (

    SELECT DISTINCT
        id,
        contact_id,
        person_source_xf.channel_last_touch,
        person_source_xf.medium_last_touch,
        person_source_xf.source_last_touch,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY discovery_date_c,dbt_valid_from DESC ) AS mql_row
    FROM {{ref('sfdc_opportunity_snapshots')}}
    LEFT JOIN {{ref('person_source_xf')}}
        ON sfdc_opportunity_snapshots.contact_id=person_source_xf.person_id
    WHERE discovery_date_c >= '2023-01-01'
    AND type = 'New Business'
    AND is_deleted = FALSE

)

SELECT
    id,
    contact_id,
    channel_last_touch,
    medium_last_touch,
    source_last_touch
FROM base
WHERE mql_row = 1