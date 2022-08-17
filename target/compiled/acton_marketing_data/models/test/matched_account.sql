

WITH base AS (
SELECT *
FROM "acton".public."account_match"
)

SELECT
REPLACE(match_account,' ','_') AS matched_account_converted,
REPLACE(account,' ','_') AS account_converted
FROM base