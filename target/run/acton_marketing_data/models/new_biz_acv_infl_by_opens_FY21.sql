

  create  table "acton"."dbt_actonmarketing"."new_biz_acv_infl_by_opens_FY21__dbt_tmp"
  as (
    

WITH opp_with_contact_base AS (
SELECT *
FROM "acton"."dbt_actonmarketing"."opportunities_with_contacts"
--FROM "acton".dbt_actonmarketing.opportunities_with_contacts

), email_click_base AS (
    SELECT *
    FROM "acton"."dbt_actonmarketing"."email_opens_ao_xf"
    --FROM "acton".dbt_actonmarketing.email_opens_ao_xf

) , sum_base AS (
SELECT DISTINCT
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
opp_created_date,
discovery_date,
stage_name,
acv::numeric,
opp_lead_source,
type,
opportunity_id AS opps
FROM email_click_base
LEFT JOIN opp_with_contact_base ON 
email_click_base.email=opp_with_contact_base.email
WHERE opportunity_id IS NOT null
AND discovery_date >= '2021-01-01'
AND discovery_date>=action_time
AND type = 'New Business'
AND action_time >= '2021-01-01'
--AND automated_program_name IS NOT null
--AND automated_program_name != ''
--AND automated_program_name NOT LIKE '%Conf%'
--GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

), intermediate AS (
SELECT DISTINCT
automated_program_name,
acv
FROM sum_base
WHERE discovery_date IS NOT null
--AND discovery_date>=action_time
--AND automated_program_name IS NOT null
--AND automated_program_name != ''
--AND automated_program_name NOT LIKE '%Conf%'

), final AS (
SELECT
automated_program_name,
SUM(acv) AS total_acv
FROM intermediate
GROUP BY 1

)

SELECT *
FROM final
WHERE total_acv >=1
  );