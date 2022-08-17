

  create  table "acton"."dbt_actonmarketing"."funnel_report_current_week_lost__dbt_tmp"
  as (
    

WITH current_week AS (

    SELECT
        week 
    FROM "acton"."dbt_actonmarketing"."date_base_xf"
    WHERE day=CURRENT_DATE-7

), base AS (

    SELECT DISTINCT
        opp_source_xf.opportunity_id AS lost_id,
        opp_source_xf.opportunity_name,
        acv_deal_size_usd,
        opp_source_xf.close_date AS close_date,
        billing_country AS country,
        account_global_region
    FROM "acton"."dbt_actonmarketing"."opp_source_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."date_base_xf" ON
    opp_source_xf.close_date=date_base_xf.day
    LEFT JOIN "acton"."dbt_actonmarketing"."account_source_xf" ON
    opp_source_xf.account_id=account_source_xf.account_id
    LEFT JOIN current_week ON 
    date_base_xf.week=current_week.week
    WHERE current_week.week IS NOT null
    AND close_date IS NOT null
    AND discovery_date IS NOT null
    AND type = 'New Business'
    AND stage_name IN ('Closed – Lost No Resources / Budget','Closed – Lost Not Ready / No Decision','Closed – Lost Product Deficiency','Closed - Lost to Competitor')

)

SELECT *
FROM base
  );