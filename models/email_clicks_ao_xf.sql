{{ config(materialized='table') }}

with base as (
select * from "acton".public.email_clicks_ao_2021
union all
select * from "acton".public.email_clicks_ao_20220224

)
select *
from base
