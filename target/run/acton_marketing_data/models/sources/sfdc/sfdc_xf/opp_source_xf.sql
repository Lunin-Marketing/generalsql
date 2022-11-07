

  create  table "acton"."dbt_actonmarketing"."opp_source_xf__dbt_tmp"
  as (
    

WITH base AS (
SELECT *
FROM "acton"."dbt_actonmarketing"."opp_source_base"

), intermediate AS (

    SELECT
    --IDs
        base.opportunity_id,
        base.account_id,
        base.owner_id,
        contact_id,
        base.contract_id,
        ao_account_id,
        base.created_by_id,
        renewed_contract_id,
        primary_quote_id,
        lead_id_converted_from,
        contact_role_contact_id,

    --Opp Info
        opportunity_name,
        type,
        stage_name,
        owner.user_name AS owner_name, 
        opp_lead_source,
        opp_crm,
        renewal_type,
        csm,
        marketing_channel,
        opportunity_status,
        sql_status_reason,
        opp_type_details,
        renewal_stage,
        forecast_category,

    --Account Info
        account_source_xf.segment,
        account_source_xf.sdr,
        account_source_xf.industry,
        account_source_xf.target_account,
        account_source_xf.industry_bucket,
        account_source_xf.global_region AS account_global_region,
        account_source_xf.company_size_rev,
        account_source_xf.is_current_customer,
        account_source_xf.account_name,

    --Flags
        base.is_deleted,
        is_closed,
        is_won,
        
    --Opp Value    
        amount,
        renewal_acv,
        --contract_source_xf.contract_acv,
        acv_deal_size_usd,
        deal_size_range,       
        
    --Opp Dates    
        created_day,
        base.created_date,
        base.last_modified_date,
        base.systemmodstamp,
        discovery_date,
        confirmed_value_date,
        negotiation_date,
        demo_date,
        solution_date,
        closing_date,
        implement_date,
        sql_date,
        voc_date,
        discovery_day_time,
        demo_day_time,
        implement_day_time,
        sql_day_time,
        voc_day_time,
        discovery_call_date,
        close_date,
        close_day,
        discovery_call_scheduled_datetime,
        discovery_call_completed_datetime,
        sql_day,

    --Attribution Information 
        opp_channel_lead_creation,
        opp_medium_lead_creation,
        opp_channel_opportunity_creation,
        opp_medium_opportunity_creation,
        opp_content_opportunity_creation, 
        opp_source_opportunity_creation, 
        opp_channel_first_touch,
        opp_content_first_touch,
        opp_medium_first_touch,
        opp_source_first_touch,
        opp_offer_asset_type_opportunity_creation,
        opp_offer_asset_subtype_opportunity_creation,
        opp_offer_asset_topic_opportunity_creation,
        opp_offer_asset_name_opportunity_creation,
        opp_offer_asset_name_first_touch,
        opp_offer_asset_name_lead_creation, 
        opp_offer_asset_subtype_first_touch, 
        opp_offer_asset_subtype_lead_creation,
        opp_offer_asset_topic_first_touch, 
        opp_offer_asset_topic_lead_creation,
        opp_offer_asset_type_first_touch, 
        opp_offer_asset_type_lead_creation,
        opp_subchannel_first_touch,
        opp_subchannel_lead_creation, 
        opp_subchannel_opportunity_creation,
        opp_source_lead_creation,
        opp_campaign_opportunity_creation,
        opp_campaign_first_touch,
        channel_bucket
    FROM base
    LEFT JOIN "acton"."dbt_actonmarketing"."account_source_xf" ON
    base.account_id=account_source_xf.account_id
    LEFT JOIN "acton"."dbt_actonmarketing"."user_source_xf" owner ON
    base.owner_id=owner.user_id
    LEFT JOIN "acton"."dbt_actonmarketing"."contact_role_xf" ON
    base.opportunity_id=contact_role_xf.contact_role_opportunity_id

)

SELECT *
FROM intermediate
  );