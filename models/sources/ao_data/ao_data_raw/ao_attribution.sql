{{ config(materialized='table') }}

SELECT
    "attributionId",
    "attributionCookieId",
    
FROM "9883Data".FACTS.ATTRIBUTION_9883