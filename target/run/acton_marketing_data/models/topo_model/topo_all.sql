

  create  table "acton"."dbt_actonmarketing"."topo_all__dbt_tmp"
  as (
    

SELECT *
FROM "acton"."dbt_actonmarketing"."topo_closing"
UNION ALL
SELECT *
FROM "acton"."dbt_actonmarketing"."topo_demos"
UNION ALL
SELECT *
FROM "acton"."dbt_actonmarketing"."topo_leads"
UNION ALL
SELECT *
FROM "acton"."dbt_actonmarketing"."topo_lost"
UNION ALL
SELECT *
FROM "acton"."dbt_actonmarketing"."topo_mqls"
UNION ALL
SELECT *
FROM "acton"."dbt_actonmarketing"."topo_pipe"
UNION ALL
SELECT *
FROM "acton"."dbt_actonmarketing"."topo_sals"
UNION ALL
SELECT *
FROM "acton"."dbt_actonmarketing"."topo_sqls"
UNION ALL
SELECT *
FROM "acton"."dbt_actonmarketing"."topo_sqos"
UNION ALL
SELECT *
FROM "acton"."dbt_actonmarketing"."topo_voc"
UNION ALL
SELECT *
FROM "acton"."dbt_actonmarketing"."topo_won_acv"
UNION ALL
SELECT *
FROM "acton"."dbt_actonmarketing"."topo_won"
  );