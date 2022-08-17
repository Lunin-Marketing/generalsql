

  create  table "acton"."dbt_actonmarketing"."funnel_report_current_quarter_leads_ss__dbt_tmp"
  as (
    

WITH current_quarter AS (

    SELECT
        fy,
        quarter 
    FROM "acton"."dbt_actonmarketing"."date_base_xf"
    WHERE day=CURRENT_DATE

), base AS (

    SELECT DISTINCT
        lead_source_ss_xf.lead_id AS lead_id,
        lead_source_ss_xf.marketing_created_date AS created_date,
        week,
        global_region
    FROM "acton"."dbt_actonmarketing"."lead_source_ss_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."date_base_xf" ON
    lead_source_ss_xf.marketing_created_date=date_base_xf.day
    LEFT JOIN current_quarter ON 
    date_base_xf.quarter=current_quarter.quarter
    WHERE current_quarter.quarter IS NOT null

)

SELECT *
FROM base
  );