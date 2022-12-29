
  
    

        create or replace transient table AO_MARKETING.dbt_snowflake.date_base_xf  as
        (
SELECT
day::date AS day,
week,
month,
month_name,
quarter,
fy
FROM AO_MARKETING.public.date_base
        );
      
  