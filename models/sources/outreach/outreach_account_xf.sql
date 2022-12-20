{{ config(materialized='table') }}

SELECT *
FROM {{source('outreach','prospect')}}