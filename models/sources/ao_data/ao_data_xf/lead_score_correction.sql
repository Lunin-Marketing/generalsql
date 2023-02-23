{{ config(materialized='table') }}

WITH ao_combined AS (

    SELECT
        email,
        action_day,
        CASE
            WHEN action = 'Content Syndication - Content Download' THEN 20
            WHEN action = 'registered for an event' THEN 5
            WHEN action = 'attended an event' THEN 50
            WHEN action = 'Event Handraiser' THEN 50
        END AS lead_score
    FROM {{ref('ao_ctp_xf')}}
    UNION ALL
    SELECT
        email,
        action_day,
        3 AS lead_score
    FROM {{ref('ao_emails_xf')}}
    UNION ALL
    SELECT
        email,
        action_day,
        20 AS lead_score
    FROM {{ref('ao_forms_xf')}}
    UNION ALL
    SELECT
        email,
        action_day,
        CASE
            WHEN action = 'REGISTERED' THEN -10
            WHEN action = 'ATTENDED' THEN 50
        END AS lead_score
    FROM {{ref('ao_webinars_xf')}}
    UNION ALL
    SELECT
        email,
        action_day,
        1 AS lead_score
    FROM {{ref('ao_webpages_xf')}}

), person_base AS (

    SELECT
        person_id,
        email,
        CASE
            WHEN employee_count > 0 AND employee_count <= 10 THEN 40
            WHEN employee_count > 10 AND employee_count <= 30 THEN 30
            WHEN employee_count > 30 AND employee_count <= 50 THEN 20
        END AS negative_score
    FROM {{ref('person_source_xf')}}
    WHERE mql_most_recent_date >= '2023-02-22'

), person_final AS (

    SELECT
        person_id,
        email,
        CASE
            WHEN negative_score IS NULL THEN 0
            ELSE negative_score
        END AS negative_score
        -- SUM(negative_score) AS negative_score
    FROM person_base
    -- GROUP BY 1,2

), running_total AS (

    SELECT
        person_id,
        action_day,
        SUM(lead_score) AS total_lead_score
    FROM person_final
    LEFT JOIN ao_combined
        ON person_final.email=ao_combined.email
    WHERE lead_score IS NOT null
    GROUP BY 1,2
    ORDER BY 2,1

), running_total_intermediate AS (

    SELECT
        person_id,
        action_day,
        SUM(total_lead_score) AS lead_score,
        ROW_NUMBER () OVER (PARTITION BY person_id ORDER BY action_day) AS score_row
    FROM running_total
    GROUP BY 1,2
    ORDER BY 2

), running_total_final AS (

    SELECT
        person_id,
        action_day,
        lead_score,
        score_row
    FROM running_total_intermediate
    WHERE lead_score >= 50

), score_final AS (

    SELECT
        person_id,
        action_day,
        lead_score
    FROM running_total_final
    WHERE score_row = 1

), intermediate AS (

    SELECT
        score_final.person_id,
        score_final.lead_score,
        negative_score,
        SUM(score_final.lead_score - negative_score) AS final_lead_score
    FROM score_final
    LEFT JOIN person_final
        ON score_final.person_id=person_final.person_id
    GROUP BY 1,2,3

), final AS (

    SELECT
        intermediate.person_id,
        score_final.action_day,
        intermediate.final_lead_score
    FROM intermediate
    LEFT JOIN score_final
        ON intermediate.person_id=score_final.person_id
    WHERE final_lead_score >= 50

) 

SELECT *
FROM final
