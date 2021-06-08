{{ config(materialized='table') }}

SELECT
lead_id,
email,
lead_source,
is_converted,
is_hand_raiser,
mql_created_date,
mql_most_recent_date,
lead_status
FROM "acton".dbt_actonmarketing.lead_source_xf
WHERE mql_created_date IS NOT null
AND lead_owner != 'AO-Fake Leads'
AND email NOT LIKE '%act-on.com'
AND lead_source = 'Marketing'
