

SELECT DISTINCT
   person_id
FROM "acton"."dbt_actonmarketing"."person_source_xf"
WHERE mql_most_recent_date >= '2022-08-01'
AND mql_most_recent_date <= '2022-09-30'