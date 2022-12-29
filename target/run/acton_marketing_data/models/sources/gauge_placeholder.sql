
  
    

        create or replace transient table AO_MARKETING.dbt_snowflake.gauge_placeholder  as
        (
SELECT *
FROM AO_MARKETING.public.gauge_placeholder
        );
      
  