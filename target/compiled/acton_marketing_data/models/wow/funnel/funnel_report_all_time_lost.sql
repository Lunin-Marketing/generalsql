

WITH base AS (

        SELECT DISTINCT
        opp_source_xf.opportunity_id AS lost_id,
        CONCAT('https://acton.my.salesforce.com/',opp_source_xf.opportunity_id) AS lost_url,
        opp_source_xf.account_name,
        acv_deal_size_usd,
        opp_source_xf.close_date AS lost_date,
        CASE
        WHEN opp_source_xf.account_global_region IS null THEN 'blank'
        ELSE opp_source_xf.account_global_region
    END AS account_global_region,
    CASE
        WHEN opp_source_xf.company_size_rev IS null THEN 'blank'
        ELSE opp_source_xf.company_size_rev
    END AS company_size_rev,
    CASE
        WHEN opp_source_xf.opp_lead_source IS null THEN 'blank'
        ELSE opp_source_xf.opp_lead_source
    END AS opp_lead_source,
    CASE
        WHEN opp_source_xf.segment IS null THEN 'blank'
        ELSE opp_source_xf.segment
    END AS segment,
    CASE
        WHEN opp_source_xf.industry IS null THEN 'blank'
        ELSE opp_source_xf.industry
    END AS industry,
    CASE
        WHEN industry_bucket IS null THEN 'blank'
        ELSE industry_bucket
    END AS industry_bucket,
    CASE
        WHEN opp_source_xf.channel_bucket IS null THEN 'blank'
        ELSE opp_source_xf.channel_bucket
    END AS channel_bucket
    FROM "acton"."dbt_actonmarketing"."opp_source_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."account_source_xf" ON
    opp_source_xf.account_id=account_source_xf.account_id
    WHERE opp_source_xf.close_date IS NOT null
    AND opp_source_xf.type = 'New Business'
    AND opp_source_xf.stage_name IN ('Closed – Lost No Resources / Budget','Closed – Lost Not Ready / No Decision','Closed – Lost Product Deficiency','Closed - Lost to Competitor')

)

SELECT *
FROM base