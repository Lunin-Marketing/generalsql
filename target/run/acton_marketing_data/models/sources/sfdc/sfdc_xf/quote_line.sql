

  create  table "acton"."dbt_actonmarketing"."quote_line__dbt_tmp"
  as (
    

WITH base AS (

    SELECT *
    FROM "acton".salesforce."sbqq_quote_line_c"

), final AS (

    SELECT
        base.id AS quote_id,
        base.is_deleted,
        base.name AS quote_name,
        base.currency_iso_code,
        DATE_TRUNC('day',base.created_date)::Date AS quote_created_date,
        DATE_TRUNC('day',base.last_modified_date)::Date AS quote_last_modified_date,
        base.system_modstamp AS systemmodstamp,
        base.sbqq_quote_c AS sbqq_quote,
        base.sbqq_product_option_c AS sbqq_product_option,
        base.sbqq_product_c AS sbqq_product,
        base.sbqq_prorate_multiplier_c AS sbqq_prorate_multiplier,
        base.sbqq_prorated_list_price_c AS sbqq_prorated_list_price,
        base.sbqq_prorated_price_c AS sbqq_prorated_price,
        base.customer_discount_c AS customer_discount,
        base.sbqq_product_subscription_type_c AS sbqq_product_subscription_type,
        base.sbqq_subscription_term_c AS sbqq_product_subscription_term,
        sbqq_quote_c.sbqq_opportunity_2_c AS opportunity_id,
        product_xf.sbqq_subscription_type,
        product_xf.product_code,
        sbqq_quote_c.sbqq_primary_c AS sbqq_primary_quote
    FROM base
    LEFT JOIN "acton".salesforce."sbqq_quote_c" ON
    base.id=sbqq_quote_c.id
    LEFT JOIN "acton"."dbt_actonmarketing"."product_xf" ON
    base.sbqq_product_c=product_xf.product_id
    WHERE base.is_deleted = 'False'

)   

SELECT *
FROM final
  );