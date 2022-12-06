

WITH base AS (

    SELECT DISTINCT
        opportunity_id AS pipeline_id,
        CONCAT('https://acton.my.salesforce.com/',opportunity_id) AS pipeline_url,
        opp_source_xf.close_date,
        created_date,
        opp_source_xf.account_name,
        opportunity_name,
        stage_name,
        type AS opp_type,
        owner_name,
        sdr_name,
        acv_deal_size_usd AS acv,
        CASE
            WHEN account_source_xf.is_current_customer IS null THEN false
            ELSE account_source_xf.is_current_customer
        END AS is_current_customer,
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
            WHEN opp_source_xf.target_account IS null THEN false
            ELSE opp_source_xf.target_account
        END AS target_account,
        CASE
            WHEN channel_bucket_details IS null THEN 'blank'
            ELSE channel_bucket_details
        END AS channel_bucket_details,
        CASE
            WHEN opp_source_xf.channel_bucket IS null THEN 'blank'
            ELSE opp_source_xf.channel_bucket
        END AS channel_bucket
    FROM "acton"."dbt_actonmarketing"."opp_source_xf"
    LEFT JOIN "acton"."dbt_actonmarketing"."account_source_xf" ON
    opp_source_xf.account_id=account_source_xf.account_id
    WHERE opp_source_xf.close_date IS NOT null
    AND stage_name NOT IN ('Closed - Duplicate','Closed - Admin Removed')
    AND opp_source_xf.opportunity_id IS NOT null
    AND is_closed = false
    AND opp_source_xf.close_date > CURRENT_DATE

) , final AS (

    SELECT DISTINCT
        pipeline_id,
        close_date,
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
        target_account,
        channel_bucket,
        channel_bucket_details,
        CASE
            WHEN stage_name = 'SQL' THEN '0.SQL'
            WHEN stage_name = 'Discovery' THEN '1.SQO'
            WHEN LOWER(stage_name) like '%demo%' THEN '2.Demo'
            WHEN stage_name = 'Champion Confirmed' THEN '3.Champion'
            WHEN stage_name = 'VOC/Negotiate' THEN '4.VOC'
            WHEN stage_name = 'Closing' THEN '5.Closing'
            WHEN stage_name = 'Implement' THEN '6.Closed Won'
            WHEN LOWER(stage_name) LIKE '%lost%' THEN '7.Closed Lost'
            WHEN stage_name = 'Renewed' THEN '8.Renewed'
            WHEN stage_name = 'Not Renewed' THEN '9.Not Renewed'
            WHEN stage_name = 'SQL - No Opportunity' THEN '10.SQL - No Opp'
            ELSE stage_name
        END AS current_stage
    FROM base

)

SELECT *
FROM final