WITH base AS (

SELECT *
FROM {{ source('salesforce', 'lead') }}

), intermediate AS (

SELECT
    created_date::Timestamp AS created_date
FROM base
WHERE email ='100443596@alumnos.uc3m.es'

)

SELECT
    {{ dbt_date.convert_timezone("created_date", "America/Los_Angeles") }} As create_date
FROM intermediate
