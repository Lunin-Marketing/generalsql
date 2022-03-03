{{ config(materialized='table') }}

WITH current_week AS (

    SELECT
        week 
    FROM {{ref('date_base_xf')}}
    WHERE day=CURRENT_DATE-14

), base AS (

    SELECT DISTINCT
        contract_source_xf.contract_id AS contract_id,
        arr_loss_amount,
        contract_source_xf.churn_date AS churn_date,
        account_global_region AS country
    FROM {{ref('contract_source_xf')}}
    LEFT JOIN {{ref('date_base_xf')}} ON
    contract_source_xf.churn_date=date_base_xf.day
    LEFT JOIN {{ref('account_source_xf')}} ON
    contract_source_xf.account_id=account_source_xf.account_id
    LEFT JOIN current_week ON 
    date_base_xf.week=current_week.week
    WHERE current_week.week IS NOT null
    AND churn_date IS NOT null
    AND status = 'Activated'
    AND contract_status = 'Cancelled'
    AND cs_churn = 'true'

)

SELECT *
FROM base
