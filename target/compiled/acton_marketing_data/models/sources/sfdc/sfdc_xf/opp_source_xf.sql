

WITH base AS (
SELECT *
FROM "acton"."salesforce"."opportunity"

), intermediate AS (

    SELECT DISTINCT
        base.id AS opportunity_id,
        base.is_deleted,
        base.account_id,
        account.name AS account_name,
        --account.account_csm_name,
        --account.account_owner_name,
        account.current_customer_c AS is_current_customer,
        account.sdr_c AS sdr_id,
        sdr.user_name AS sdr_name,
        CASE
            WHEN account.billing_country IS NOT null AND account.billing_country IN ('GB','UK','IE','DE','DK','FI','IS','NO','SE','FR','AL','AD','AM','AT','BY','BE','BA','BG','HR','CS','CY','CZ','EE','FX','GE','GR','HU','IT','LV','LI','LT','LU','MK','MT','MD','MC','ME','NL','PL','PT','RO','SM','RS','SJ','SK','SI','ES','CH','UA','VA','FO','GI','GG','IM','JE','XK','RU') THEN 'EUROPE'
            WHEN account.billing_country IS NOT null AND account.billing_country IN ('JP','KR','CN','MN','TW','VN','HK','LA','TH','KH','PH','MY','SG','ID','LK','IN','NP','BT','MM','PK','AF','KG','UZ','TM','KZ') THEN 'APJ'
            WHEN account.billing_country IS NOT null AND account.billing_country IN ('AU','CX','NZ','NF','Australia','New Zealand') THEN 'AUNZ'
            WHEN account.billing_country IS NOT null AND account.billing_country IN ('AR','BO','BR','BZ','CL','CO','CU','CR','DO','EC','FK','GF','GS','GY','GT','HN','MX','NI','PA','PE','PR','PY','SR','SV','UY','VE')THEN 'LATAM'
            WHEN account.billing_state IS NOT null AND account.billing_state IN ('CA','NV','UT','AK','MO','CO','HI','OK','IL','AR','NE','MI','KS','OR','WA','ID','WI','MN','ND','SD','MT','WY','IA','NB','ON','PE','QC','AB','BC','MB','SK','NL','NS','YT','NU','NT') THEN 'NA-WEST'
            WHEN account.billing_state IS NOT null AND account.billing_state IN ('NY','CT','MA','VT','NH','ME','NJ','RI','TX','AZ','NM','MS','LA','AL','TN','KY','OH','IN','GA','FL','NC','SC','PA','DC','DE','MD','VA','WV') THEN 'NA-EAST'
            WHEN account.billing_country IS NOT null AND account.billing_country IN ('AG','AI','AN','AW','BB','BM','BS','DM','GD','GP','HT','JM','KN','LC','MQ','MS','TC','TT','VC','VG','VI') THEN 'NA-EAST'
            WHEN account.billing_country IS NOT null AND account.billing_country IN ('US','CA') AND account.billing_state IS null  THEN 'NA-NO-STATEPROV'
            WHEN account.billing_country IS NOT null AND account.billing_state IS NOT null THEN 'ROW'
            ELSE 'Unknown'
        END AS account_global_region,
        CASE 
            WHEN account.annual_revenue <= 49999999 THEN 'SMB'
            WHEN account.annual_revenue > 49999999 AND account.annual_revenue <= 499999999 THEN 'Mid-Market'
            WHEN account.annual_revenue > 499999999 THEN 'Enterprise'
        END AS company_size_rev,
        base.name AS opportunity_name,
        stage_name,
        amount,
        base.type,
        lead_source AS opp_lead_source,
        is_closed,
        is_won,
        base.owner_id,
        owner.user_name AS owner_name, 
        base.created_date AS created_day,
        DATE_TRUNC('day',base.created_date)::Date AS created_date,
        DATE_TRUNC('day',base.last_modified_date)::Date AS last_modified_date,
        base.system_modstamp AS systemmodstamp,
        contact_id,
        base.contract_id,
        crm_c AS opp_crm,
        renewal_type_c AS renewal_type,
        renewal_acv_value_c AS renewal_acv,
        channel_lead_creation_c AS opp_channel_lead_creation,
        medium_lead_creation_c AS opp_medium_lead_creation,
        DATE_TRUNC('day',discovery_date_c)::Date AS discovery_date,
        DATE_TRUNC('day',date_reached_confirmed_value_c)::Date AS confirmed_value_date,
        DATE_TRUNC('day',date_reached_contract_c)::Date AS negotiation_date,
        DATE_TRUNC('day',date_reached_demo_c)::Date AS demo_date,
        DATE_TRUNC('day',date_reached_solution_c)::Date AS solution_date,
        DATE_TRUNC('day',date_reached_closing_c)::Date AS closing_date,
        DATE_TRUNC('day',date_time_reached_implement_c)::Date AS implement_date,
        DATE_TRUNC('day',sql_date_c)::Date AS sql_date,
        DATE_TRUNC('day',date_time_reached_voc_negotiate_c)::Date AS voc_date,
        DATE_TRUNC('day',date_time_reached_discovery_c)::Date AS discovery_day_time,
        DATE_TRUNC('day',date_time_reached_demo_c)::Date AS demo_day_time,
        DATE_TRUNC('day',date_time_reached_implement_c)::Date AS implement_day_time,
        DATE_TRUNC('day',date_time_reached_sql_c)::Date AS sql_day_time,
        DATE_TRUNC('day',date_time_reached_voc_negotiate_c)::Date AS voc_day_time,
        oc_utm_channel_c AS opp_channel_opportunity_creation,
        oc_utm_medium_c AS opp_medium_opportunity_creation,
        oc_utm_content_c AS opp_content_opportunity_creation, 
        oc_utm_source_c AS opp_source_opportunity_creation, 
        base.csm_c AS csm,
        base.marketing_channel_c AS marketing_channel,
        base.ft_utm_channel_c AS opp_channel_first_touch,
        base.ft_utm_content_c AS opp_content_first_touch,
        base.ft_utm_medium_c AS opp_medium_first_touch,
        base.ft_utm_source_c AS opp_source_first_touch,
        base.oc_offer_asset_type_c AS opp_offer_asset_type_opportunity_creation,
        base.oc_offer_asset_subtype_c AS opp_offer_asset_subtype_opportunity_creation,
        base.oc_offer_asset_topic_c AS opp_offer_asset_topic_opportunity_creation,
        base.oc_offer_asset_name_c AS opp_offer_asset_name_opportunity_creation,
        base.ft_offer_asset_name_c AS opp_offer_asset_name_first_touch,
        base.lc_offer_asset_name_c  AS opp_offer_asset_name_lead_creation, 
        base.ft_offer_asset_subtype_c AS opp_offer_asset_subtype_first_touch, 
        base.lc_offer_asset_subtype_c AS opp_offer_asset_subtype_lead_creation,
        base.ft_offer_asset_topic_c AS opp_offer_asset_topic_first_touch, 
        base.lc_offer_asset_topic_c AS opp_offer_asset_topic_lead_creation,
        base.ft_offer_asset_type_c AS opp_offer_asset_type_first_touch, 
        base.lc_offer_asset_type_c AS opp_offer_asset_type_lead_creation,
        base.ft_subchannel_c AS opp_subchannel_first_touch,
        base.lc_subchannel_c AS opp_subchannel_lead_creation, 
        base.oc_subchannel_c AS opp_subchannel_opportunity_creation,
        DATE_TRUNC('day',base.discovery_call_scheduled_date_c)::Date AS discovery_call_date,
        base.opportunity_status_c AS opportunity_status,
        base.sql_status_reason_c AS sql_status_reason,
        DATE_TRUNC('day',base.sql_date_c)::Date AS sql_day,
        DATE_TRUNC('day',base.discovery_call_scheduled_date_time_c)::Date AS discovery_call_scheduled_datetime,
        DATE_TRUNC('day',base.discovery_call_completed_date_time_c)::Date AS discovery_call_completed_datetime,
        base.ao_account_id_c AS ao_account_id,
        base.lead_id_converted_from_c AS lead_id_converted_from,
        base.close_date,
        base.opportunity_type_detail_c AS opp_type_details,
        DATE_TRUNC('day',base.close_date)::Date AS close_day,
        base.source_lead_creation_c AS opp_source_lead_creation,
        base.oc_utm_campaign_c AS opp_campaign_opportunity_creation,
        base.forecast_category,
        base.ft_utm_campaign_c AS opp_campaign_first_touch,  
        base.acv_deal_size_override_c AS acv_deal_size_override,
        base.lead_grade_at_conversion_c AS lead_grade_at_conversion,
        base.renewal_stage_c AS renewal_stage,
        base.created_by_id,
        quota_credit_renewal_c AS quota_credit_renewal,
        base.sbqq_renewed_contract_c AS renewed_contract_id,
        quota_credit_c AS quota_credit,
        sbqq_primary_quote_c AS primary_quote_id,
        quota_credit_new_business_c AS quota_credit_new_business,
        quota_credit_one_time_c AS quota_credit_one_time,
        submitted_for_approval_c AS submitted_for_approval,
        acv_add_back_c AS acv_add_back,
        trigger_renewal_value_c AS trigger_renewal_value,
        opportunity_line_item_xf.sbqq_subscription_type,
        quote_line.sbqq_product_subscription_term,
        opportunity_line_item_xf.product_code,
        opportunity_line_item_xf.product_family,
        opportunity_line_item_xf.total_price,
        opportunity_line_item_xf.annual_price,
        quote_line.sbqq_primary_quote,
        contract_source_xf.contract_acv,
        account.industry,
        CASE
            WHEN account.industry IN ('Business Services') THEN 'Business Services'
            WHEN account.industry IN ('Finance','Insurance') THEN 'Finance'
            WHEN account.industry IN ('Manufacturing') THEN 'Manufacturing'
            WHEN account.industry IN ('Software','Telecommunications') THEN 'SoftCom'
            ELSE 'Other'
        END AS industry_bucket,
        --account.segment,
        CASE
            WHEN base.type IN ('New Business','Upsell','Trigger Renewal','Trigger Up','Non-Monetary Mod','Admin Opp','Partner New','Partner UpSell','Admin Conversion') THEN true
            ELSE false
        END AS include_in_acv_deal_size,
        CASE
            WHEN is_closed = true THEN 
        ((base.close_date)::date - (base.created_date)::date)
    
            ELSE 
        ((CURRENT_DATE)::date - (base.created_date)::date)
    
        END AS age,
        CASE
            WHEN opportunity_line_item_xf.sbqq_subscription_type = 'Renewable' AND opportunity_line_item_xf.product_code != 'SOW-MNTH-CUST' THEN SUM(total_price)
            ELSE 0
        END AS tcv,
        CASE 
            WHEN opportunity_line_item_xf.sbqq_subscription_type = 'Renewable' AND opportunity_line_item_xf.product_family != 'Bundle' AND opportunity_line_item_xf.product_code NOT IN ('SOW-MNTH-CUST','MAAS-FND-TRAIN','MAAS-FND-FAST','MAAS-ACC-DELIV','MAAS-ACC-STRAT')
                THEN SUM(annual_price)
            ELSE 0
        END AS acv,
        CASE 
            WHEN opportunity_line_item_xf.sbqq_subscription_type = 'One-time' AND opportunity_line_item_xf.product_code NOT IN ('STD-CPCTY-ONE','SOW-MKTR-DMD','SOW-MNTH-CUST','MAAS-FND-TRAIN','MAAS-FND-FASTTRK','MAAS-ACC-IMPL','MAAS-ACC-DELIV','MAAS-ACC-STRAT','AO-CPM-OVER')
                THEN SUM(total_price)
            ELSE 0
        END AS one_time_ps_value,
        CASE 
            WHEN opportunity_line_item_xf.sbqq_subscription_type = 'One-time' AND opportunity_line_item_xf.product_code IN ('STD-CPCTY-ONE','AO-CPM-OVER') THEN SUM(total_price)
            ELSE 0
        END AS one_time_license_value,
        CASE 
            WHEN opportunity_line_item_xf.product_code IN ('SOW-MKTR-DMD','SOW-MNTH-CUST','MAAS-FND-TRAIN','MAAS-FND-FASTTRK','MAAS-ACC-IMPL','MAAS-ACC-DELIV','MAAS-ACC-STRAT') THEN SUM(total_price)
            ELSE 0
        END AS pso_recurring_fees
    FROM base
    LEFT JOIN "acton"."dbt_actonmarketing"."contract_source_xf" ON
    base.id=contract_source_xf.contract_opportunity_id
    LEFT JOIN "acton"."dbt_actonmarketing"."opportunity_line_item_xf" ON
    base.id=opportunity_line_item_xf.opportunity_id
    LEFT JOIN "acton"."dbt_actonmarketing"."quote_line" ON
    base.id=quote_line.opportunity_id
    LEFT JOIN "acton".salesforce."account" account ON
    base.account_id=account.id
    LEFT JOIN "acton"."dbt_actonmarketing"."user_source_xf" owner ON
    base.owner_id=owner.user_id
    LEFT JOIN "acton"."dbt_actonmarketing"."user_source_xf" sdr ON
    account.sdr_c=sdr.user_id
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108

), intermediate_acv_formula AS (

    SELECT 
      intermediate.opportunity_id,
      intermediate.is_deleted,
      intermediate.account_id,
      intermediate.account_name,
      intermediate.is_current_customer,
      intermediate.sdr_id,
      intermediate.sdr_name,
      intermediate.company_size_rev,
      intermediate.account_global_region,
      intermediate.opportunity_name,
      intermediate.stage_name,
      intermediate.amount,
      intermediate.type,
      intermediate.opp_lead_source,
      intermediate.is_closed,
      intermediate.is_won,
      intermediate.owner_id,
      intermediate.owner_name,
      intermediate.created_day,
      intermediate.created_date,
      intermediate.last_modified_date,
      intermediate.systemmodstamp,
      intermediate.contact_id,
      intermediate.contract_id,
      intermediate.opp_crm,
      intermediate.renewal_type,
      intermediate.renewal_acv,
      intermediate.opp_channel_lead_creation,
      intermediate.opp_medium_lead_creation,
      intermediate.discovery_date,
      intermediate.demo_date,
      intermediate.solution_date,
      intermediate.negotiation_date,
      intermediate.confirmed_value_date,
      intermediate.closing_date,
      intermediate.implement_date,
      intermediate.sql_date,
      intermediate.voc_date,
      intermediate.discovery_day_time,
      intermediate.demo_day_time,
      intermediate.implement_day_time,
      intermediate.sql_day_time,
      intermediate.voc_day_time,
      intermediate.opp_channel_opportunity_creation,
      intermediate.opp_medium_opportunity_creation,
      intermediate.opp_content_opportunity_creation,
      intermediate.opp_source_opportunity_creation,
      intermediate.csm,
      intermediate.marketing_channel,
      intermediate.opp_channel_first_touch,
      intermediate.opp_content_first_touch,
      intermediate.opp_medium_first_touch,
      intermediate.opp_source_first_touch,
      intermediate.opp_offer_asset_type_opportunity_creation,
      intermediate.opp_offer_asset_subtype_opportunity_creation,
      intermediate.opp_offer_asset_topic_opportunity_creation,
      intermediate.opp_offer_asset_name_opportunity_creation,
      intermediate.opp_offer_asset_name_first_touch,
      intermediate.opp_offer_asset_name_lead_creation,
      intermediate.opp_offer_asset_subtype_first_touch,
      intermediate.opp_offer_asset_subtype_lead_creation,
      intermediate.opp_offer_asset_topic_first_touch,
      intermediate.opp_offer_asset_topic_lead_creation,
      intermediate.opp_offer_asset_type_first_touch,
      intermediate.opp_offer_asset_type_lead_creation,
      intermediate.opp_subchannel_first_touch,
      intermediate.opp_subchannel_lead_creation,
      intermediate.opp_subchannel_opportunity_creation,
      intermediate.discovery_call_date,
      intermediate.opportunity_status,
      intermediate.sql_status_reason,
      intermediate.sql_day,
      intermediate.discovery_call_scheduled_datetime,
      intermediate.discovery_call_completed_datetime,
      intermediate.ao_account_id,
      intermediate.lead_id_converted_from,
      intermediate.close_date,
      intermediate.opp_type_details,
      intermediate.close_day,
      intermediate.opp_source_lead_creation,
      intermediate.opp_campaign_opportunity_creation,
      intermediate.forecast_category,
      intermediate.opp_campaign_first_touch,
      intermediate.acv_deal_size_override,
      intermediate.lead_grade_at_conversion,
      intermediate.renewal_stage,
      intermediate.created_by_id,
      intermediate.quota_credit_renewal,
      intermediate.renewed_contract_id,
      intermediate.quota_credit,
      intermediate.primary_quote_id,
      intermediate.quota_credit_new_business,
      intermediate.quota_credit_one_time,
      intermediate.submitted_for_approval,
      intermediate.acv_add_back,
      intermediate.trigger_renewal_value,
      intermediate.sbqq_primary_quote,
      intermediate.sbqq_product_subscription_term,
      intermediate.contract_acv,
      intermediate.industry,
      intermediate.industry_bucket,
      --intermediate.segment,
      intermediate.include_in_acv_deal_size,
      intermediate.age,
      intermediate.tcv,
      intermediate.acv,
      intermediate.one_time_ps_value,
      intermediate.one_time_license_value,
      intermediate.pso_recurring_fees,
      intermediate.total_price,
      intermediate.annual_price,
      CASE
        WHEN sbqq_product_subscription_term = 0 AND sbqq_primary_quote IS null THEN SUM(contract_acv)
        WHEN sbqq_product_subscription_term > 0 AND sbqq_product_subscription_term < 12 THEN SUM(tcv) 
        ELSE SUM(acv)
      END AS acv_formula
      --intermediate.sbqq_subscription_type,
      --intermediate.product_code,
      --intermediate.product_family,
    FROM intermediate
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110

), intermediate_acv_sum AS (
    
    SELECT
      intermediate_acv_formula.opportunity_id,
      intermediate_acv_formula.is_deleted,
      intermediate_acv_formula.account_id,
      intermediate_acv_formula.account_name,
      intermediate_acv_formula.is_current_customer,
      intermediate_acv_formula.sdr_id,
      intermediate_acv_formula.sdr_name,
      intermediate_acv_formula.account_global_region,
      intermediate_acv_formula.company_size_rev,
      intermediate_acv_formula.opportunity_name,
      intermediate_acv_formula.stage_name,
      intermediate_acv_formula.amount,
      intermediate_acv_formula.type,
      intermediate_acv_formula.opp_lead_source,
      intermediate_acv_formula.is_closed,
      intermediate_acv_formula.is_won,
      intermediate_acv_formula.owner_id,
      intermediate_acv_formula.owner_name,
      intermediate_acv_formula.created_day,
      intermediate_acv_formula.created_date,
      intermediate_acv_formula.last_modified_date,
      intermediate_acv_formula.systemmodstamp,
      intermediate_acv_formula.contact_id,
      intermediate_acv_formula.contract_id,
      intermediate_acv_formula.opp_crm,
      intermediate_acv_formula.renewal_type,
      intermediate_acv_formula.renewal_acv,
      intermediate_acv_formula.opp_channel_lead_creation,
      intermediate_acv_formula.opp_medium_lead_creation,
      intermediate_acv_formula.discovery_date,
      intermediate_acv_formula.negotiation_date,
      intermediate_acv_formula.demo_date,
      intermediate_acv_formula.solution_date,
      intermediate_acv_formula.confirmed_value_date,
      intermediate_acv_formula.closing_date,
      intermediate_acv_formula.implement_date,
      intermediate_acv_formula.discovery_day_time,
      intermediate_acv_formula.demo_day_time,
      intermediate_acv_formula.implement_day_time,
      intermediate_acv_formula.sql_day_time,
      intermediate_acv_formula.voc_day_time,
      intermediate_acv_formula.sql_date,
      intermediate_acv_formula.voc_date,
      intermediate_acv_formula.opp_channel_opportunity_creation,
      intermediate_acv_formula.opp_medium_opportunity_creation,
      intermediate_acv_formula.opp_content_opportunity_creation,
      intermediate_acv_formula.opp_source_opportunity_creation,
      intermediate_acv_formula.csm,
      intermediate_acv_formula.marketing_channel,
      intermediate_acv_formula.opp_channel_first_touch,
      intermediate_acv_formula.opp_content_first_touch,
      intermediate_acv_formula.opp_medium_first_touch,
      intermediate_acv_formula.opp_source_first_touch,
      intermediate_acv_formula.opp_offer_asset_type_opportunity_creation,
      intermediate_acv_formula.opp_offer_asset_subtype_opportunity_creation,
      intermediate_acv_formula.opp_offer_asset_topic_opportunity_creation,
      intermediate_acv_formula.opp_offer_asset_name_opportunity_creation,
      intermediate_acv_formula.opp_offer_asset_name_first_touch,
      intermediate_acv_formula.opp_offer_asset_name_lead_creation,
      intermediate_acv_formula.opp_offer_asset_subtype_first_touch,
      intermediate_acv_formula.opp_offer_asset_subtype_lead_creation,
      intermediate_acv_formula.opp_offer_asset_topic_first_touch,
      intermediate_acv_formula.opp_offer_asset_topic_lead_creation,
      intermediate_acv_formula.opp_offer_asset_type_first_touch,
      intermediate_acv_formula.opp_offer_asset_type_lead_creation,
      intermediate_acv_formula.opp_subchannel_first_touch,
      intermediate_acv_formula.opp_subchannel_lead_creation,
      intermediate_acv_formula.opp_subchannel_opportunity_creation,
      intermediate_acv_formula.discovery_call_date,
      intermediate_acv_formula.opportunity_status,
      intermediate_acv_formula.sql_status_reason,
      intermediate_acv_formula.sql_day,
      intermediate_acv_formula.discovery_call_scheduled_datetime,
      intermediate_acv_formula.discovery_call_completed_datetime,
      intermediate_acv_formula.ao_account_id,
      intermediate_acv_formula.lead_id_converted_from,
      intermediate_acv_formula.close_date,
      intermediate_acv_formula.opp_type_details,
      intermediate_acv_formula.close_day,
      intermediate_acv_formula.opp_source_lead_creation,
      intermediate_acv_formula.opp_campaign_opportunity_creation,
      intermediate_acv_formula.forecast_category,
      intermediate_acv_formula.opp_campaign_first_touch,
      intermediate_acv_formula.acv_deal_size_override,
      intermediate_acv_formula.lead_grade_at_conversion,
      intermediate_acv_formula.renewal_stage,
      intermediate_acv_formula.created_by_id,
      intermediate_acv_formula.quota_credit_renewal,
      intermediate_acv_formula.renewed_contract_id,
      intermediate_acv_formula.quota_credit,
      intermediate_acv_formula.primary_quote_id,
      intermediate_acv_formula.quota_credit_new_business,
      intermediate_acv_formula.quota_credit_one_time,
      intermediate_acv_formula.submitted_for_approval,
      intermediate_acv_formula.acv_add_back,
      intermediate_acv_formula.trigger_renewal_value,
      intermediate_acv_formula.sbqq_primary_quote,
      intermediate_acv_formula.sbqq_product_subscription_term,
      intermediate_acv_formula.contract_acv,
      intermediate_acv_formula.industry,
      intermediate_acv_formula.industry_bucket,
     -- intermediate_acv_formula.segment,
      intermediate_acv_formula.include_in_acv_deal_size,
      intermediate_acv_formula.age,
      SUM(intermediate_acv_formula.one_time_ps_value) AS one_time_ps_value,
      SUM(intermediate_acv_formula.one_time_license_value) AS one_time_license_value,
      SUM(intermediate_acv_formula.pso_recurring_fees) AS pso_recurring_fees,
      SUM(intermediate_acv_formula.tcv) AS tcv,
      SUM(intermediate_acv_formula.acv) AS acv,
      SUM(intermediate_acv_formula.total_price) AS total_price,
      SUM(intermediate_acv_formula.annual_price) AS annual_price,
      SUM(intermediate_acv_formula.acv_formula) AS acv_formula
    FROM intermediate_acv_formula
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103

), intermediate_acv_deal_size AS (
    
    SELECT
      intermediate_acv_sum.*,
      CASE
        WHEN account_global_region IN ('EUROPE','ROW','AUNZ') THEN 'EMEA'
        WHEN company_size_rev IN ('SMB') OR company_size_rev IS null THEN 'Velocity'
        WHEN company_size_rev IN ('Mid-Market','Enterprise') THEN 'Upmarket'
        ELSE null
      END AS segment, 
      CASE
        WHEN acv_deal_size_override > 0 AND is_closed = false AND submitted_for_approval = false THEN acv_deal_size_override
        WHEN type = 'Renewal' THEN SUM(acv_add_back + trigger_renewal_value)
        WHEN include_in_acv_deal_size=false THEN 0
        WHEN include_in_acv_deal_size=true AND acv_formula=0 AND one_time_ps_value=0 AND one_time_license_value=0 AND pso_recurring_fees=0 THEN amount
        ELSE acv_formula
      END AS acv_deal_size_usd
    FROM intermediate_acv_sum
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111

), final AS (

    SELECT
        intermediate_acv_deal_size.*,
        CASE 
            WHEN acv_deal_size_usd <= '9999' THEN '< 10K'
            WHEN acv_deal_size_usd > '9999' AND acv_deal_size_usd <= '14999' THEN '10-15K'
            WHEN acv_deal_size_usd > '14999' AND acv_deal_size_usd <= '19999' THEN '15-20K'
            WHEN acv_deal_size_usd > '19999' AND acv_deal_size_usd <= '24999' THEN '20-25K'
            WHEN acv_deal_size_usd > '24999' AND acv_deal_size_usd <= '29999' THEN '25-30K'
            ELSE '30K+'
        END AS deal_size_range,
        CASE 
            WHEN LOWER(opp_channel_lead_creation) = 'organic' THEN 'Organic'
            WHEN LOWER(opp_channel_lead_creation) IS null THEN 'Unknown'
            WHEN LOWER(opp_channel_lead_creation) = 'social' AND LOWER(opp_medium_lead_creation) = 'social-organic' THEN 'Social - Organic'
            WHEN LOWER(opp_channel_lead_creation) = 'social' AND LOWER(opp_medium_lead_creation) = 'social-paid' THEN 'Paid Social'
            WHEN LOWER(opp_channel_lead_creation) = 'ppc' THEN 'PPC/Paid Search'
            WHEN LOWER(opp_channel_lead_creation) = 'email' AND LOWER(opp_source_lead_creation) like '%act-on%' THEN 'Paid Email' 
            WHEN LOWER(opp_channel_lead_creation) = 'ppl' AND LOWER(opp_medium_lead_creation) = 'syndication partner' THEN 'PPL'
            WHEN LOWER(opp_channel_lead_creation) IN ('prospecting','ppl') AND LOWER(opp_medium_lead_creation) = 'intent partner' THEN 'Intent Partners'
            WHEN LOWER(opp_channel_lead_creation) = 'event' THEN 'Events and Trade Shows'
            WHEN LOWER(opp_channel_lead_creation) = 'partner' THEN 'Partners'
            ELSE 'Other'
        END AS channel_bucket
    FROM intermediate_acv_deal_size
)

SELECT DISTINCT
final.*,
contact_role_contact_id
FROM final
LEFT JOIN "acton"."dbt_actonmarketing"."contact_role_xf" ON
final.opportunity_id=contact_role_xf.contact_role_opportunity_id