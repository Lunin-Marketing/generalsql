

  create  table "acton"."dbt_actonmarketing"."wow_lw_sql_attribution__dbt_tmp"
  as (
    

WITH last_week AS (

    SELECT DISTINCT
        week 
    FROM "acton"."dbt_actonmarketing"."date_base_xf"
    WHERE day = CURRENT_DATE-7

), final AS (
    
    SELECT
        opp_channel_lead_creation,
        opp_medium_lead_creation,
        opp_source_lead_creation,
        COUNT(DISTINCT sql_id) AS sqls
    FROM "acton"."dbt_actonmarketing"."sql_source_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."date_base_xf" ON
    sql_source_xf.created_date=date_base_xf.day
    LEFT JOIN last_week ON 
    date_base_xf.week=last_week.week
    WHERE last_week.week IS NOT null
    AND created_date IS NOT null
    AND stage_name NOT IN ('Closed - Duplicate','Closed - Admin Removed')
    AND type = 'New Business'
    GROUP BY 1,2,3

)

SELECT
    opp_channel_lead_creation,
    opp_medium_lead_creation,
    opp_source_lead_creation,
    SUM(sqls) AS sqls
FROM final
GROUP BY 1,2,3
  );