

  create  table "acton"."dbt_actonmarketing"."wow_summary_conversion_delta__dbt_tmp"
  as (
    

WITH last_week AS (

    SELECT 
        wow_summary_lw_conversion.*,
        'week' AS week
    FROM "acton"."dbt_actonmarketing"."wow_summary_lw_conversion"

), previous_week AS (

    SELECT 
        wow_summary_pw_conversion.*,
        'week' AS week
    FROM "acton"."dbt_actonmarketing"."wow_summary_pw_conversion"

), intermediate AS (
    
    SELECT
        last_lead_to_mql,
        last_mql_to_sal,
        last_sal_to_sql,
        last_sql_to_sqo,
        last_sqo_to_won,
        last_sqo_to_lost,
        previous_lead_to_mql,
        previous_mql_to_sal,
        previous_sal_to_sql,
        previous_sql_to_sqo,
        previous_sqo_to_won,
        previous_sqo_to_lost
    FROM last_week
    LEFT JOIN previous_week ON 
    last_week.week=previous_week.week

), final AS (

    SELECT
        ROUND(SUM(last_lead_to_mql-previous_lead_to_mql),2) AS lead_to_mql_delta,
        ROUND(SUM(last_mql_to_sal-previous_mql_to_sal),2) AS mql_to_sal_delta,
        ROUND(SUM(last_sal_to_sql-previous_sal_to_sql),2) AS sal_to_sql_delta,
        ROUND(SUM(last_sql_to_sqo-previous_sql_to_sqo),2) AS sql_to_sqo_delta,
        ROUND(SUM(last_sqo_to_won-previous_sqo_to_won),2) AS sqo_to_won_delta,
        ROUND(SUM(last_sqo_to_lost-previous_sqo_to_lost),2) AS sqo_to_lost_delta
    FROM intermediate
)

SELECT *
FROM final
  );