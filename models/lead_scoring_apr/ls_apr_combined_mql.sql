{{ config(materialized='table') }}

WITH apr_1 AS (

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
        FROM {{ref('ls_apr_1')}}
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

), apr_2 AS (

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
        FROM {{ref('ls_apr_2')}}
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

), apr_3 AS (

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
        FROM {{ref('ls_apr_3')}}
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

), apr_4 AS (

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
        FROM {{ref('ls_apr_4')}}
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

), apr_5 AS (

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
        FROM {{ref('ls_apr_5')}}
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

), apr_6 AS (

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
        FROM {{ref('ls_apr_6')}}
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

), apr_7 AS (

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
        FROM {{ref('ls_apr_7')}}
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

), apr_8 AS (

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
        FROM {{ref('ls_apr_8')}}
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

), apr_9 AS (

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
        FROM {{ref('ls_apr_9')}}
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

), apr_10 AS (

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
        FROM {{ref('ls_apr_10')}}
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

), apr_11 AS (

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
        FROM {{ref('ls_apr_11')}}
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

), apr_12 AS (

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
        FROM {{ref('ls_apr_12')}}
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

), apr_13 AS (

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
        FROM {{ref('ls_apr_13')}}
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

), apr_14 AS (

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
        FROM {{ref('ls_apr_14')}}
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

), apr_15 AS (

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
        FROM {{ref('ls_apr_15')}}
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

), apr_16 AS (

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
        FROM {{ref('ls_apr_16')}}
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

), apr_17 AS (

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
        FROM {{ref('ls_apr_17')}}
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

), apr_18 AS (

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
        FROM {{ref('ls_apr_18')}}
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

), apr_19 AS (

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
        FROM {{ref('ls_apr_19')}}
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

), apr_20 AS (

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
        FROM {{ref('ls_apr_20')}}
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

), apr_21 AS (

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
        FROM {{ref('ls_apr_21')}}
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

), apr_22 AS (

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
        FROM {{ref('ls_apr_22')}}
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

), apr_23 AS (

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
        FROM {{ref('ls_apr_23')}}
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

), apr_24 AS (

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
        FROM {{ref('ls_apr_24')}}
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

), apr_25 AS (

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
        FROM {{ref('ls_apr_25')}}
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

), apr_26 AS (

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
        FROM {{ref('ls_apr_26')}}
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

), apr_27 AS (

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
        FROM {{ref('ls_apr_27')}}
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

), combined AS (

    SELECT DISTINCT
        email,
        score_date,
        '2023-04-01' AS model_date
    FROM apr_1
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-02' AS model_date
    FROM apr_2
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-03' AS model_date
    FROM apr_3
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-04' AS model_date
    FROM apr_4
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-05' AS model_date
    FROM apr_5
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-06' AS model_date
    FROM apr_6
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-07' AS model_date
    FROM apr_7
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-08' AS model_date
    FROM apr_8
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-09' AS model_date
    FROM apr_9
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-10' AS model_date
    FROM apr_10
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-11' AS model_date
    FROM apr_11
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-12' AS model_date
    FROM apr_12
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-13' AS model_date
    FROM apr_13
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-14' AS model_date
    FROM apr_14
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-15' AS model_date
    FROM apr_15
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-16' AS model_date
    FROM apr_16
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-17' AS model_date
    FROM apr_17
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-18' AS model_date
    FROM apr_18
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-19' AS model_date
    FROM apr_19
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-20' AS model_date
    FROM apr_20
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-21' AS model_date
    FROM apr_21
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-22' AS model_date
    FROM apr_22
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-23' AS model_date
    FROM apr_23
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-24' AS model_date
    FROM apr_24
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-25' AS model_date
    FROM apr_25
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-26' AS model_date
    FROM apr_26
    UNION ALL
    SELECT DISTINCT
        email,
        score_date,
        '2023-04-27' AS model_date
    FROM apr_27
    
), final AS (

    SELECT DISTINCT
        email,
        score_date,
        model_date,
        ROW_NUMBER()OVER(PARTITION BY email ORDER BY model_date ASC) AS row_count
    FROM combined

)

SELECT 
    email,
    score_date
FROM final
WHERE row_count = 1