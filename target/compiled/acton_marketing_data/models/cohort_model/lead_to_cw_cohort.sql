

WITH base AS (

    SELECT *
    FROM "acton"."dbt_actonmarketing"."opportunities_with_contacts"

), intermediate AS (

    SELECT
        person_id,
        opportunity_id,
        company_size_rev,
        global_region,
        segment,
        channel_bucket,
        marketing_created_date::Date,
        mql_created_date::Date,
        CASE 
            WHEN person_status NOT IN ('Current Customer','Partner','Bad Data','No Fit') THEN mql_created_date::Date
            ELSE null
        END AS sal_created_date,
        person_status,
        opp_created_date::Date,
        discovery_date::Date,
        close_date::Date,
        CASE
            WHEN is_won = true THEN close_date::Date
            ELSE null
        END AS cw_date,
        CASE 
            WHEN is_won = false AND is_closed = true THEN close_date::Date
            ELSE null
        END AS cl_date,
        is_won,
        channel_lead_creation,
        medium_lead_creation,
        source_lead_creation,
        is_mql,
        is_sal,
        is_sql,
        is_sqo,
        is_cl,
        is_cw
    FROM base
    WHERE marketing_created_date >= '2021-01-01'
    --AND person_status != 'Current Customer'

), final AS (

    SELECT 
        intermediate.*,
        CASE 
            WHEN mql_created_date>=marketing_created_date THEN 

    
        ((mql_created_date)::date - (marketing_created_date)::date)
    

 
            ELSE 0 
        END AS days_to_mql,
        CASE 
            WHEN sal_created_date>=mql_created_date THEN 

    
        ((sal_created_date)::date - (mql_created_date)::date)
    

 
            ELSE 0 
        END AS  days_to_sal,
        CASE 
            WHEN opp_created_date>=sal_created_date THEN 

    
        ((opp_created_date)::date - (sal_created_date)::date)
    

 
            ELSE 0 
        END AS  days_to_sql,
        CASE 
            WHEN discovery_date>=opp_created_date THEN 

    
        ((discovery_date)::date - (opp_created_date)::date)
    

 
            ELSE 0 
        END AS  days_to_sqo,
        CASE 
            WHEN cw_date>=discovery_date THEN 

    
        ((cw_date)::date - (discovery_date)::date)
    

 
            ELSE 0 
        END AS  days_to_won,
        CASE 
            WHEN cl_date>=discovery_date THEN 

    
        ((cl_date)::date - (discovery_date)::date)
    

 
            ELSE 0 
        END AS  days_to_closed_lost
    FROM intermediate

)

SELECT *
FROM final