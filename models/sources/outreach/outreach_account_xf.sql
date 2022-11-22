{{ config(materialized='table') }}

SELECT
    id
FROM {{source('outreach','account')}}