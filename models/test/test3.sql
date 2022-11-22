{{ config(materialized='table') }}

SELECT
    person_id,
    industry,
    industry_bucket
FROM {{ref('person_source_xf')}}