

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

    --Flages
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


        
          
        --acv_deal_size_override,
        --lead_grade_at_conversion,
        
        
        -- quota_credit_renewal,
        
        -- quota_credit,
        
        -- quota_credit_new_business,
        -- quota_credit_one_time,
        -- submitted_for_approval,
        -- acv_add_back,
        -- trigger_renewal_value,
        -- opportunity_line_item_xf.sbqq_subscription_type,
        -- quote_line.sbqq_product_subscription_term,
        -- opportunity_line_item_xf.product_code,
        -- opportunity_line_item_xf.product_family,
        -- opportunity_line_item_xf.total_price,
        -- opportunity_line_item_xf.annual_price,
        -- quote_line.sbqq_primary_quote,
    FROM base
    -- LEFT JOIN "acton"."dbt_actonmarketing"."contract_source_xf" ON
    -- base.opportunity_id=contract_source_xf.contract_opportunity_id
    -- LEFT JOIN "acton"."dbt_actonmarketing"."opportunity_line_item_xf" ON
    -- base.opportunity_id=opportunity_line_item_xf.opportunity_id
    -- LEFT JOIN "acton"."dbt_actonmarketing"."quote_line" ON
    -- base.opportunity_id=quote_line.opportunity_id
    LEFT JOIN "acton"."dbt_actonmarketing"."user_source_xf" owner ON
    base.owner_id=owner.user_id
    LEFT JOIN "acton"."dbt_actonmarketing"."contact_role_xf" ON
    base.opportunity_id=contact_role_xf.contact_role_opportunity_id

)

SELECT *
FROM intermediate