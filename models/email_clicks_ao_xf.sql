{{ config(materialized='table') }}

with base as (
select * from "acton".public.email_clicks_ao_2020
union all
select * from "acton".public.email_clicks_ao_20210603

)
select *
from base
