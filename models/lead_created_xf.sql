{{ config(materialized='table') }}
SELECT
lead_id,
email,
lead_source,
is_hand_raiser,
lead_status,
lead_created_date
FROM "acton".dbt_acton.lead_source_xf