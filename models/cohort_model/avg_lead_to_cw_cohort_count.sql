WITH base AS(

    SELECT *
    FROM {{ ref('lead_to_cw_cohort')}}
)

SELECT DISTINCT
    channel_bucket,
    segment,
    COUNT(DISTINCT mql_id) AS mqls,
    COUNT(DISTINCT sal_id) AS sals,
    COUNT(DISTINCT sql_id) AS sqls,
    COUNT(DISTINCT sqo_id) AS sqos,
    COUNT(DISTINCT cl_id) AS closed_lost,
    COUNT(DISTINCT cw_id) AS closed_won
FROM base
GROUP BY 1,2