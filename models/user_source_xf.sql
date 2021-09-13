{{ config(materialized='table') }}
WITH base AS (
SELECT *
FROM "acton".public."User"
)
SELECT
"Id" AS user_id,
"LastName" AS user_last_name,
"FirstName" AS user_first_name,
"Name" AS user_name,
"IsActive" AS is_active_user
FROM base