{{ config(materialized='table') }}

WITH opp_with_contact_base AS (
SELECT *
FROM "acton".dbt_acton.opportunities_with_contacts
), email_click_base AS (
    SELECT *
    FROM "acton".dbt_acton.email_clicks_ao_xf
) , final AS (
SELECT 
email_click_base.email,
action_time,
message_title,
automated_program_name,
campaign_name,
is_current_customer,
is_hand_raiser,
mql_created_date,
opportunity_id,
close_date,
is_won,
created_date,
discovery_date,
stage_name,
acv,
opp_lead_source,
type
FROM email_click_base
LEFT JOIN opp_with_contact_base ON 
email_click_base.email=opp_with_contact_base.email
WHERE opportunitY_id IS NOT null
limit 10
)
SELECT *
FROM final