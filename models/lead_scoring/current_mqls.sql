{{ config(materialized='table') }}

WITH base AS (

    SELECT 
        email,
        score_date,
        total_lead_score,
        ROW_NUMBER ()OVER(PARTITION BY email ORDER BY score_date ASC) AS row_count
    FROM {{ref('ao_scoring_xf')}}
    WHERE total_lead_score >= 50

)

SELECT
    email,
    score_date,
    total_lead_score
FROM base
WHERE row_count = 1
ORDER BY 3 ASC