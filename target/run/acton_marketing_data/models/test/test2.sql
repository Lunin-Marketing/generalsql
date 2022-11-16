

  create  table "acton"."dbt_actonmarketing"."test2__dbt_tmp"
  as (
    

SELECT
    person_id,
    mql_most_recent_date,
    looking_for_ma,
    is_hand_raiser
FROM "acton"."dbt_actonmarketing"."person_source_xf"
WHERE lead_score = 50
AND (LOWER(looking_for_ma) = 'yes'
    OR LOWER(is_hand_raiser) = 'true')
AND mql_most_recent_date IS NOT null
  );