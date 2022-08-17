

WITH base AS (

    SELECT
        person_id,
        email,
        global_region,
        marketing_created_date,
        mql_most_recent_date,
        campaign_first_touch,
        campaign_last_touch,
        campaign_lead_creation,
        offer_asset_name_first_touch,
        offer_asset_name_last_touch,
        offer_asset_name_lead_creation,
        offer_asset_subtype_first_touch,
        offer_asset_subtype_last_touch,
        offer_asset_subtype_lead_creation,
        offer_asset_topic_first_touch,
        offer_asset_topic_last_touch,
        offer_asset_topic_lead_creation,
        offer_asset_type_first_touch,
        offer_asset_type_last_touch,
        offer_asset_type_lead_creation
    FROM "acton"."dbt_actonmarketing"."person_source_xf"
    WHERE (campaign_first_touch LIKE 'marketo ap'
        OR campaign_last_touch LIKE 'marketo ap'
        OR campaign_lead_creation LIKE 'marketo ap')
    
)

SELECT *
FROM base