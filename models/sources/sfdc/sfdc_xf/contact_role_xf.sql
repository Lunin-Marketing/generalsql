{{ config(materialized='table') }}

WITH base AS (
SELECT *
FROM {{ source('salesforce', 'opportunity_contact_role') }}

), intermediate AS (

    SELECT
        opportunity_id AS contact_role_opportunity_id,
        contact_id AS contact_role_contact_id,
        CASE
            WHEN role IS null THEN 'None'
            ELSE role
        END AS contact_role,
        is_primary AS contact_role_is_primary
    FROM base
    WHERE base.is_deleted = 'False'

)

SELECT *
FROM intermediate