{{ config(materialized='table') }}

SELECT DISTINCT
   mql_most_recent_date
FROM {{ref('person_source_xf')}}
-- WHERE mql_most_recent_date >= '2022-08-01'
-- AND mql_most_recent_date <= '2022-09-30'
WHERE person_id = '0031O000030lAB9QAM'