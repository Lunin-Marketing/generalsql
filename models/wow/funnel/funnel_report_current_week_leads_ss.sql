{{ config(materialized='table') }}

WITH current_week AS (

    SELECT
        week 
    FROM {{ref('date_base_xf')}}
    WHERE day=CURRENT_DATE-7

), base AS (

    SELECT DISTINCT
        lead_source_ss_xf.lead_id AS lead_id,
        lead_source_ss_xf.marketing_created_date AS created_date,
        country
    FROM {{ref('lead_source_ss_xf')}}
    LEFT JOIN {{ref('date_base_xf')}} ON
    lead_source_ss_xf.marketing_created_date=date_base_xf.day
    LEFT JOIN current_week ON 
    date_base_xf.week=current_week.week
    WHERE current_week.week IS NOT null
    AND marketing_created_date IS NOT null
    AND lead_owner != '00Ga0000003Nugr' -- AO-Fake Leads
    AND email NOT LIKE '%act-on.com'
    AND lead_source = 'Marketing'

)

SELECT *
FROM base