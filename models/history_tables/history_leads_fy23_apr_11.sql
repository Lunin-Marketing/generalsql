{{ config(materialized='table') }}

WITH base AS (

    SELECT DISTINCT
        lead_id,
        email,
        mql_most_recent_date,
        ROW_NUMBER()OVER(PARTITION BY lead_id ORDER BY mql_most_recent_date DESC) AS row_count
    FROM {{ref('history_sfdc_lead_xf')}}
    WHERE dbt_updated_at::Date <= '2023-04-11'
    AND mql_most_recent_date IS NOT null
    ORDER BY 1

)

SELECT DISTINCT
    lead_id,
    email,
    mql_most_recent_date
FROM base 
WHERE row_count = 1
ORDER BY 3 DESC
