
  
    

        create or replace transient table AO_MARKETING.dbt_snowflake.opportunity_line_item_xf  as
        (

WITH base AS (
SELECT *
FROM AO_MARKETING.salesforce.opportunity_line_item

), final AS (
    SELECT 
        id AS line_item_id,
        opportunity_id,
        product_2_id AS product_id,
        name AS line_item_name,
        currency_iso_code,
        discount,
        total_price,
        list_price,
        DATE_TRUNC('day',created_date)::Date AS line_item_created_date,
        DATE_TRUNC('day',last_modified_date)::Date AS last_modified_date,
        system_modstamp AS systemmodstamp,
        is_deleted,
        sbqq_subscription_type_c AS sbqq_subscription_type,
        line_item_mrr_c AS line_item_mrr,
        product_code_c AS product_code,
        annual_price_c AS annual_price,
        discounted_annual_price_c AS discounted_annual_price,
        monthly_gross_c AS monthly_gross,
        monthly_net_c AS monthly_net,
        subscription_type_c AS subscription_type,
        total_discount_c AS total_discount,
        prorate_multiplier_mirror_c AS prorate_multiplier_mirror,
        sbqq_parent_id_c AS sbqq_parent_id,
        opp_product_family_text_c AS product_family
    FROM base
    WHERE base.is_deleted = 'False'

)

SELECT *
FROM final
        );
      
  