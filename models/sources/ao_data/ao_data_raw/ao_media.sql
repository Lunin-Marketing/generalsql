{{ config(materialized='table') }}

WITH base AS (

    SELECT
    1 AS unique_visitor_id,
    1 AS e_mail_address
    -- SELECT *
    -- FROM {{ source('data_studio_s3', 'data_studio_media') }}

)

SELECT *
FROM base