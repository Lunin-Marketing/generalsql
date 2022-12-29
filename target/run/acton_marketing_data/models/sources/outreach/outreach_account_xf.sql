
  
    

        create or replace transient table AO_MARKETING.dbt_snowflake.outreach_account_xf  as
        (

SELECT *
FROM AO_MARKETING.outreach.prospect
        );
      
  