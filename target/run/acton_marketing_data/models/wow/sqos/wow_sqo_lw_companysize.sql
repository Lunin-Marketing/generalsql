

  create  table "acton"."dbt_actonmarketing"."wow_sqo_lw_companysize__dbt_tmp"
  as (
    

WITH last_week AS (

    SELECT DISTINCT
        week 
    FROM "acton"."dbt_actonmarketing"."date_base_xf"
    WHERE day = CURRENT_DATE-7

), final AS (
    
    SELECT
        opp_source_xf.company_size_rev,
        COUNT(DISTINCT opportunity_id) AS sqos
    FROM "acton"."dbt_actonmarketing"."opp_source_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."account_source_xf" ON
    opp_source_xf.account_id=account_source_xf.account_id
    LEFT JOIN "acton"."dbt_actonmarketing"."date_base_xf" ON
    opp_source_xf.discovery_date=date_base_xf.day
    LEFT JOIN last_week ON 
    date_base_xf.week=last_week.week
    WHERE last_week.week IS NOT null
    AND discovery_date IS NOT null
    AND stage_name NOT IN ('Closed - Duplicate','Closed - Admin Removed')
    AND type = 'New Business'
    GROUP BY 1

)

SELECT
company_size_rev,
SUM(sqos) AS sqos
FROM final
GROUP BY 1
  );