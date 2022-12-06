

SELECT
    opportunity_id
FROM "acton"."dbt_actonmarketing"."opp_source_xf"
WHERE channel_bucket = 'Unknown'
AND created_date >= '2022-06-01'
AND type = 'New Business'