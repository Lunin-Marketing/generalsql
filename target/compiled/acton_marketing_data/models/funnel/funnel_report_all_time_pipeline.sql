

WITH base AS (

    SELECT DISTINCT
        opportunity_id AS sqo_id,
        CONCAT('https://acton.my.salesforce.com/',opportunity_id) AS sqo_url,
        discovery_date AS sqo_date,
        created_date AS created_date,
        opp_source_xf.account_name,
        opportunity_name,
        stage_name,
        type AS opp_type,
        owner_name,
        sdr_name,
        close_date,
        acv_deal_size_usd AS acv,
        CASE
        WHEN account_global_region IS null THEN 'blank'
        ELSE account_global_region
    END AS account_global_region,
    CASE
        WHEN opp_source_xf.company_size_rev IS null THEN 'blank'
        ELSE opp_source_xf.company_size_rev
    END AS company_size_rev,
    CASE
        WHEN opp_lead_source IS null THEN 'blank'
        ELSE opp_lead_source
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
        WHEN opp_source_xf.industry_bucket IS null THEN 'blank'
        ELSE opp_source_xf.industry_bucket
    END AS industry_bucket,
    CASE
        WHEN opp_source_xf.channel_bucket IS null THEN 'blank'
        ELSE opp_source_xf.channel_bucket
    END AS channel_bucket
    FROM "acton"."dbt_actonmarketing"."opp_source_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."account_source_xf" ON
    opp_source_xf.account_id=account_source_xf.account_id
    WHERE discovery_date IS NOT null
    AND stage_name NOT IN ('Closed - Duplicate','Closed - Admin Removed')

) , final AS (

    SELECT DISTINCT
        sqo_id,
        sqo_date,
        created_date,
        opportunity_name,
        stage_name,
        opp_type,
        acv,
        account_global_region,
        company_size_rev,
        opp_lead_source,
        segment,
        industry,
        industry_bucket,
        channel_bucket,
        CASE
            WHEN stage_name = 'Discovery' THEN '1.SQO'
            WHEN stage_name = 'Demo' THEN '2.Demo'
            WHEN stage_name = 'VOC/Negotiate' THEN '3.VOC'
            WHEN stage_name = 'Closing' THEN '4.Closing'
            WHEN stage_name = 'Implement' THEN '5.Closed Won'
            WHEN LOWER(stage_name) LIKE '%lost%' THEN '6.Closed Lost'
            WHEN stage_name = 'Not Renewed' THEN '7.Not Renewed'
        END AS current_stage
    FROM base

)

SELECT *
FROM final