{{ config(materialized='table') }}

WITH base AS (

    SELECT DISTINCT
        sal_source_xf.person_id AS sal_id,
        CONCAT('https://acton.my.salesforce.com/',sal_source_xf.person_id) AS sal_url,
        sal_source_xf.working_date AS sal_date,
        sal_source_xf.mql_most_recent_date AS mql_date,
        person_status,
        person_owner_name,
        company,
        most_recent_salesloft_cadence,
        is_hand_raiser,
        is_sal_after_mql,
        abm_campaign_initial,
        abm_campaign_most_recent,
        abm_date_time_initial,
        abm_date_time_most_recent,
        is_abm,
        CASE
            WHEN is_current_customer IS null THEN false
            ELSE is_current_customer
        END AS is_current_customer,
        CASE
            WHEN global_region IS null THEN 'blank'
            ELSE global_region
        END AS global_region,
        CASE
            WHEN company_size_rev IS null THEN 'blank'
            ELSE company_size_rev
        END AS company_size_rev,
        CASE
            WHEN lead_source IS null THEN 'blank'
            ELSE lead_source
        END AS lead_source,
        CASE
            WHEN segment IS null THEN 'blank'
            ELSE segment
        END AS segment,
        CASE
            WHEN industry IS null THEN 'blank'
            ELSE industry
        END AS industry,
        CASE
            WHEN industry_bucket IS null THEN 'blank'
            ELSE industry_bucket
        END AS industry_bucket,
        CASE
            WHEN channel_bucket IS null THEN 'blank'
            ELSE channel_bucket
        END AS channel_bucket,
        CASE
            WHEN channel_bucket_details IS null THEN 'blank'
            ELSE channel_bucket_details
        END AS channel_bucket_details,
        CASE
            WHEN target_account IS null THEN false
            ELSE target_account
        END AS target_account,
        CASE
            WHEN offer_asset_name_lead_creation IS null THEN 'blank'
            ELSE offer_asset_name_lead_creation
        END AS offer_asset_name_lead_creation,
        CASE
            WHEN campaign_lead_creation IS null THEN 'blank'
            ELSE campaign_lead_creation
        END AS campaign_lead_creation
    FROM {{ref('sal_source_xf')}}

)

SELECT *
FROM base