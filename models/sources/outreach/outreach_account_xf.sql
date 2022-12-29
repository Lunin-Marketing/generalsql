{{ config(materialized='table') }}

SELECT *
FROM {{source('aws_outreach','prospect')}}