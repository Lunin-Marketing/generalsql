create view opp_source_xf AS (
select * from "acton".public.opportunity_source_20210514
limit 10
)