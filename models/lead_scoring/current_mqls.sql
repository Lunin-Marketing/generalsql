{{ config(materialized='table') }}

WITH base AS (

    SELECT 
        email,
        score_date,
        total_lead_score,
        demo_lead_score,
        engagement_score,
        CASE
            WHEN total_lead_score >= 20 THEN SUM (total_lead_score+demo_lead_score+engagement_score)
            ELSE SUM(total_lead_score)
        END AS final_lead_score
    FROM {{ref('ao_scoring_xf')}}
    WHERE score_date >= CURRENT_DATE - 1
    GROUP BY 1,2,3,4,5

), final AS (

    SELECT
        email,
        score_date,
        final_lead_score,
        ROW_NUMBER ()OVER(PARTITION BY email ORDER BY score_date ASC) AS row_count
    FROM base
    WHERE final_lead_score >= 50
    ORDER BY 2,3 ASC


)

SELECT
    email,
    score_date,
    final_lead_score
FROM final
WHERE row_count = 1
ORDER BY 3 ASC
