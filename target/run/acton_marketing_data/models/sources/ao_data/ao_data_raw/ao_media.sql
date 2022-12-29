
  
    

        create or replace transient table AO_MARKETING.dbt_snowflake.ao_media  as
        (

WITH base AS (

    SELECT *
    FROM AO_MARKETING.data_studio_s3.data_studio_media

)

SELECT *
FROM base
        );
      
  