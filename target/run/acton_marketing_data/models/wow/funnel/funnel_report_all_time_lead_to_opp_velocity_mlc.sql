

  create  table "acton"."dbt_actonmarketing"."funnel_report_all_time_lead_to_opp_velocity_mlc__dbt_tmp"
  as (
    

WITH base AS (

    SELECT
        person_id,
        marketing_created_date,
        opportunity_id,
        opp_created_date,
        type AS opp_type,
        opp_segment
    FROM "acton"."dbt_actonmarketing"."opportunities_with_contacts"

), final AS (

    SELECT
        person_id,
        marketing_created_date,
        opp_created_date,
        opp_type,
        opp_segment,
        

    
        ((opp_created_date)::date - (marketing_created_date)::date)
    

 AS lead_to_opp_velocity
    FROM base
    WHERE opp_created_date >= marketing_created_date
)

SELECT
opp_segment,
opp_type,
DATE_TRUNC('year',marketing_created_date) AS marketing_created_year,
AVG(lead_to_opp_velocity)
FROM final
GROUP BY 1,2,3
  );