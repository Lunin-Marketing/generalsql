{{ config(materialized='table') }}

WITH base AS (

    SELECT
        person_id,
        marketing_created_date,
        opportunity_id,
        opp_created_date,
        type AS opp_type,
        opp_segment
    FROM {{ref('opportunities_with_contacts')}}
    WHERE type='New Business'

), final AS (

    SELECT
        person_id,
        marketing_created_date,
        opp_created_date,
        opp_segment,
        {{ dbt_utils.datediff("marketing_created_date","opp_created_date",'day')}} AS lead_to_opp_velocity
    FROM base
    WHERE opp_created_date >= marketing_created_date
    
)

SELECT
opp_segment,
DATE_TRUNC('year',marketing_created_date) AS marketing_created_year,
AVG(lead_to_opp_velocity) AS avg_lead_to_opp_velocity
FROM final
GROUP BY 1,2