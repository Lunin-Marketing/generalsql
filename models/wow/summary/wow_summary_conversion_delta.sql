{{ config(materialized='table') }}

WITH last_week AS (

    SELECT 
        wow_summary_lw_conversion.*,
        'week' AS week
    FROM {{ref('wow_summary_lw_conversion')}}

), previous_week AS (

    SELECT 
        wow_summary_pw_conversion.*,
        'week' AS week
    FROM {{ref('wow_summary_pw_conversion')}}

), intermediate AS (
    
    SELECT
        last_lead_to_mql,
        last_mql_to_sal,
        last_sal_to_sql,
        last_sql_to_sqo,
        last_sqo_to_won,
        last_sqo_to_lost,
        last_sqo_to_churn,
        previous_lead_to_mql,
        previous_mql_to_sal,
        previous_sal_to_sql,
        previous_sql_to_sqo,
        previous_sqo_to_won,
        previous_sqo_to_lost,
        previous_sqo_to_churn
    FROM last_week
    LEFT JOIN previous_week ON 
    last_week.week=previous_week.week

), final AS (

    SELECT
        SUM(last_lead_to_mql/NULLIF(previous_lead_to_mql,0)) AS lead_to_mql_delta,
        SUM(last_mql_to_sal/NULLIF(previous_mql_to_sal,0)) AS mql_to_sal_delta,
        SUM(last_sal_to_sql/NULLIF(previous_sal_to_sql,0)) AS sal_to_sql_delta,
        SUM(last_sql_to_sqo/NULLIF(previous_sql_to_sqo,0)) AS sql_to_sqo_delta,
        SUM(last_sqo_to_won/NULLIF(previous_sqo_to_won,0)) AS sqo_to_won_delta,
        SUM(last_sqo_to_lost/NULLIF(previous_sqo_to_lost,0)) AS sqo_to_lost_delta,
        SUM(last_sqo_to_churn/NULLIF(previous_sqo_to_churn,0)) AS sqo_to_churn_delta
    FROM intermediate
)

SELECT *
FROM final
