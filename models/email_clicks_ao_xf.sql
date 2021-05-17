{{ config(materialized='table') }}

with base as (
select * from "defaultdb".public.email_clicks_ao_2020
union all
select * from "defaultdb".public.email_clicks_ao_20210517

)
select *
from base
