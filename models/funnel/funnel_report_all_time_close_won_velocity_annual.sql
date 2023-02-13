{{ config(materialized='table') }}

WITH closed_won AS (

    SELECT
        opp_sales_source_xf.opportunity_id AS won_id,
        opp_sales_source_xf.close_date AS won_date,
        segment AS won_segment,
        type AS won_type
    FROM {{ref('opp_sales_source_xf')}}

),  created_opp AS (

    SELECT
        sql_source_xf.sql_id,
        sql_source_xf.created_date AS sql_date,
        segment AS opp_segment,
        type AS opp_type
    FROM {{ref('sql_source_xf')}}
    
), final AS (

    SELECT
        won_id,
        sql_date,
        won_date,
        won_segment,
        won_type,
        {{ datediff("sql_date","won_date",'day')}} AS cw_velocity
    FROM closed_won
    LEFT JOIN created_opp ON 
    closed_won.won_id=created_opp.sql_id
)

SELECT
won_segment,
won_type,
DATE_TRUNC('year',won_date) AS cw_year,
AVG(cw_velocity) AS avg_cw_velocity
FROM final
GROUP BY 1,2,3