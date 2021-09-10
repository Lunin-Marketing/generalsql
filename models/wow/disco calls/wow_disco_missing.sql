{{ config(materialized='table') }}

WITH base AS (
SELECT
opportunity_id,


FROM "acton".dbt_actonmarketing.opp_source_xf

)