{{ config(materialized='table') }}

with opp_base as (
    select *
    FROM {{ref('opp_source_xf')}}
    --from "acton".dbt_actonmarketing.opp_source_xf
), contact_base as (
    select *
    FROM {{ref('contact_source_xf')}}
    --from "acton".dbt_actonmarketing.contact_source_xf
)
select 
email,
is_current_customer, 
is_hand_raiser,
mql_created_date,
opportunity_id,
close_date,
is_won,
contact_base.created_date AS contact_created_date,
opp_base.created_date AS opp_created_date,
discovery_date,
stage_name,
acv,
opp_lead_source,
type
FROM contact_base
LEFT JOIN opp_base ON
LEFT(contact_base.account_id,15)=LEFT(opp_base.account_id,15)
WHERE opportunity_id IS NOT null
ORDER BY 5
