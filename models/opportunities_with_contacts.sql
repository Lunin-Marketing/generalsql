with opp_base as (
    select *
    from "acton".dbt_acton.opp_source_xf
), contact_base as (
    select *
    from "acton".dbt_acton.contact_source_xf
)
select 
count (distinct opportunity_id) as opps, --11343
count(distinct contact_id) as contacts --480000
from contact_base
left join opp_base on
contact_base.account_id=opp_base.account_id
where opportunity_id is not null