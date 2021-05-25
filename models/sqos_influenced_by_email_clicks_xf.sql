{{ config(materialized='table') }}

WITH opp_with_contact_base AS (
SELECT *
FROM "defaultdb".dbt_actonmarketing.opportunities_with_contacts

), email_click_base AS (
    SELECT *
    FROM "defaultdb".dbt_actonmarketing.email_clicks_ao_xf

) , sum_base AS (
SELECT 
email_click_base.email,
action_time,
message_title,
automated_program_name,
campaign_name,
is_current_customer,
is_hand_raiser,
mql_created_date,
close_date,
is_won,
created_date,
discovery_date,
stage_name,
acv,
opp_lead_source,
type,
opportunity_id AS opps
FROM email_click_base
LEFT JOIN opp_with_contact_base ON 
email_click_base.email=opp_with_contact_base.email
WHERE opportunity_id IS NOT null
--GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

), final AS (
SELECT
automated_program_name,
message_title,
campaign_name,
stage_name,
discovery_date,
type,
opps,
acv
FROM sum_base
WHERE discovery_date IS NOT null
AND discovery_date>=action_time
AND automated_program_name IS NOT null
AND automated_program_name != ''
AND automated_program_name NOT LIKE '%Conf%'
)

SELECT *
FROM final