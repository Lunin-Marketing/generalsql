{{ config(materialized='table') }}

WITH base AS (

SELECT *
FROM {{ source('salesforce', 'user') }}

)

    SELECT
        base.id AS user_id,
        username AS user_name,
        last_name AS user_last_name,
        first_name AS user_first_name,
        base.name AS user_full_name,
        profile.name AS profile_name,
        department AS user_department,
        title AS user_title,
        email AS user_email,
        phone AS user_phone,
        is_active AS is_active_user,
        manager_id,
        base.created_date AS user_created_date,
        base.last_modified_date AS user_last_modified_date,
        base.system_modstamp AS systemmodstamp,
        contact_id AS user_contact_id,
        account_id AS user_account_id,
        full_photo_url AS user_photo_url,
        sales_loft_calendar_link_c AS sales_loft_calendar_link,
        calendly_link_c AS calendly_link,
        headshot_url_c AS user_headshot_url,
        office_location_c AS user_office_location,
        sales_home_office_c AS user_sales_home_office,
        csm_type_c AS csm_type,
        my_territory_c AS user_territory,
        photo_c AS user_photo
FROM base
LEFT JOIN {{ source('salesforce', 'profile') }} ON
base.profile_id=profile.id