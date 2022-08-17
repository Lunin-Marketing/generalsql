

  create  table "acton"."dbt_actonmarketing"."wow_last_12week_mm_mql_source__dbt_tmp"
  as (
    

WITH last_12_weeks AS (

    SELECT DISTINCT
        week 
    FROM "acton"."dbt_actonmarketing"."date_base_xf"
    WHERE day BETWEEN CURRENT_DATE-84 AND CURRENT_DATE-7

), final AS (
    
    SELECT
        last_12_weeks.week,
        COUNT(person_id) AS mqls
    FROM "acton"."dbt_actonmarketing"."person_source_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."date_base_xf" ON
    person_source_xf.mql_most_recent_date=date_base_xf.day
    LEFT JOIN last_12_weeks ON 
    date_base_xf.week=last_12_weeks.week
    WHERE last_12_weeks.week IS NOT null
    AND mql_most_recent_date IS NOT null
    AND person_owner_id != '00Ga0000003Nugr' -- AO-Fake Leads
    AND email NOT LIKE '%act-on.com'
    AND lead_source = 'Marketing'
    AND company_size_rev IN ('Enterprise','Mid-Market')
    GROUP BY 1

)

SELECT
    week,
    SUM(mqls) AS mqls
FROM final
GROUP BY 1
  );