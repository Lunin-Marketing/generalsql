{{ config(materialized='table') }}

with base as (

    select *
    FROM {{ref('opportunities_with_contacts')}}

)
select
person_id,
marketing_created_date,
mql_created_date,
CASE WHEN person_status NOT IN ('Current Customer','Partner','Bad Data','No Fit') THEN mql_created_date
     ELSE null
     END AS sal_created_date,
person_status,
opp_created_date,
discovery_date,
close_date,
CASE WHEN is_won = true THEN close_date
     ELSE null
     END AS cw_date,
CASE WHEN is_won = false AND is_closed = true THEN close_date
     ELSE null
     END AS cl_date,
is_won,
channel_lead_creation,
medium_lead_creation,
source_lead_creation,
{{ dbt_utils.datediff("marketing_created_date,mql_created_date",'day') }} AS days_to_mql,
{{ dbt_utils.datediff("mql_created_date,sal_created_date",'day') }} AS days_to_sal,
{{ dbt_utils.datediff("sal_created_date,opp_created_date",'day') }} AS days_to_sql,
{{ dbt_utils.datediff("opp_created_date,discovery_date",'day') }} AS days_to_sqo,
{{ dbt_utils.datediff("discovery_date,cw_date",'day') }} AS days_to_won,
{{ dbt_utils.datediff("discovery_date,cl_date",'day') }} AS days_to_closed_lost
FROM base
WHERE marketing_created_date LIKE '2021%'