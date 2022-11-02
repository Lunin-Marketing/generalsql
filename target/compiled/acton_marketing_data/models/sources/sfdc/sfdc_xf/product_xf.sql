

WITH base AS (

    SELECT *
    FROM "acton"."salesforce"."product_2"

), final AS (

    SELECT 
        id AS product_id,
        product_code,
        DATE_TRUNC('day',created_date)::Date AS product_created_date,
        DATE_TRUNC('day',last_modified_date)::Date AS product_last_modified_date,
        system_modstamp AS systemmodstamp,
        is_deleted,
        rev_rec_c AS rev_rec,
        multiplier_override_c AS multiplier_override,
        sbqq_subscription_type_c AS sbqq_subscription_type,
        product_category_c AS product_category
    FROM base
    WHERE base.is_deleted = 'False'

)

SELECT *
FROM final