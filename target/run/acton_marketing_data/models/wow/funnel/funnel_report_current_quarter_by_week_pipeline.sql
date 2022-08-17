

  create  table "acton"."dbt_actonmarketing"."funnel_report_current_quarter_by_week_pipeline__dbt_tmp"
  as (
    

WITH base_prep AS (

    SELECT DISTINCT
        week,
        opp_lead_source
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_quarter_sqos"
    UNION ALL 
    SELECT DISTINCT
        week,
        opp_lead_source
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_quarter_demo"
    UNION ALL 
    SELECT DISTINCT
        week,
        opp_lead_source
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_quarter_voc"
    UNION ALL 
    SELECT DISTINCT
        week,
        opp_lead_source
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_quarter_closing"

), base AS (

    SELECT DISTINCT
    week,
    opp_lead_source
    FROM base_prep

), sqo_base AS (

    SELECT
        COUNT(DISTINCT sqo_id) AS sqos,
        SUM(acv) AS sqo_acv,
        week,
        opp_lead_source
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_quarter_sqos"
    GROUP BY 3,4

), demo_base AS (
    
    SELECT
        COUNT(DISTINCT demo_id) AS demo,
        SUM(acv) AS demo_acv,
        week,
        opp_lead_source
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_quarter_demo"
    GROUP BY 3,4

), voc_base AS (

    SELECT
        COUNT(DISTINCT voc_id) AS voc,
        SUM(acv) AS voc_acv,
        week,
        opp_lead_source
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_quarter_voc"
    GROUP BY 3,4

), closing_base AS (

    SELECT
        COUNT(DISTINCT closing_id) AS closing,
        SUM(acv) AS closing_acv,
        week,
        opp_lead_source
    FROM "acton"."dbt_actonmarketing"."funnel_report_current_quarter_closing"
    GROUP BY 3,4
   
), final AS (

    SELECT
        base.week,
        base.opp_lead_source,
        CASE 
            WHEN SUM(sqos) IS null THEN 0
            ELSE SUM(sqos) 
        END AS sqos,
        CASE 
            WHEN SUM(sqo_acv) IS null THEN 0
            ELSE SUM(sqo_acv) 
        END AS sqo_acv,
        CASE 
            WHEN SUM(demo) IS null THEN 0
            ELSE SUM(demo) 
        END AS demo,
        CASE 
            WHEN SUM(demo_acv) IS null THEN 0
            ELSE SUM(demo_acv) 
        END AS demo_acv,
        CASE 
            WHEN SUM(voc) IS null THEN 0
            ELSE SUM(voc) 
        END AS voc,
        CASE 
            WHEN SUM(voc_acv) IS null THEN 0
            ELSE SUM(voc_acv) 
        END AS voc_acv,
        CASE     
            WHEN SUM(closing) IS null THEN 0
            ELSE SUM(closing) 
        END AS closing,
        CASE 
            WHEN SUM(closing_acv) IS null THEN 0
            ELSE SUM(closing_acv) 
        END AS closing_acv
    FROM base
    LEFT JOIN sqo_base ON
    base.week=sqo_base.week
    AND base.opp_lead_source=sqo_base.opp_lead_source
    LEFT JOIN demo_base ON
    base.week=demo_base.week
    AND base.opp_lead_source=demo_base.opp_lead_source
    LEFT JOIN voc_base ON
    base.week=voc_base.week
    AND base.opp_lead_source=voc_base.opp_lead_source
    LEFT JOIN closing_base ON
    base.week=closing_base.week
    AND base.opp_lead_source=closing_base.opp_lead_source
    GROUP BY 1,2
    ORDER BY 1,2

)

SELECT
    week,
    opp_lead_source,
    SUM(sqos+demo+voc+closing) AS records,
    SUM(sqo_acv+demo_acv+voc_acv+closing_acv) AS pipeline
FROM final
GROUP BY 1,2
UNION ALL 
SELECT
    'Total' AS week,
    opp_lead_source,
    SUM(sqos+demo+voc+closing) AS records,
    SUM(sqo_acv+demo_acv+voc_acv+closing_acv) AS pipeline
FROM final
GROUP BY 1,2
ORDER BY 1,2
  );