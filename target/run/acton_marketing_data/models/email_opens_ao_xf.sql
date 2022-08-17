

  create  table "acton"."dbt_actonmarketing"."email_opens_ao_xf__dbt_tmp"
  as (
    

with base as (
select * from "acton".public."email_opens_ao_2020Q1"
union all
select * from "acton".public."email_opens_ao_2020Q2"
union all
select * from "acton".public."email_opens_ao_2020Q3"
union all
select * from "acton".public."email_opens_ao_2020Q4"
union all
select * from "acton".public."email_opens_ao_2021Q1"

)
select *
from base
  );