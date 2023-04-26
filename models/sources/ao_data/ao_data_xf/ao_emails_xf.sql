{{ config(materialized='table') }}

WITH email_sent AS (

    SELECT DISTINCT
        message_id,
        message_recipient_id,
        message_recipient_email,
        {{ dbt_date.convert_timezone("message_date_time", "America/Los_Angeles", "UTC") }}::Date AS message_send_date,
        TRUE AS is_sent
    FROM {{ref('ao_sent')}}
    WHERE ao_sent.message_verb = 's'
    AND message_date_time::Date >= '2022-06-01'
    AND message_id LIKE 's-%'

), email_clicked AS (

    SELECT
        message_id,
        message_recipient_id,
        message_recipient_email,
        {{ dbt_date.convert_timezone("message_date_time", "America/Los_Angeles", "UTC") }}::Date AS message_click_date,
        TRUE AS is_clicked
    FROM {{ref('ao_response')}}
    WHERE message_verb = 'c'
    AND message_date_time::Date >= '2022-06-01'
    AND message_id LIKE 's-%'

-- ), email_opened AS (

--     SELECT DISTINCT
--         message_id,
--         message_recipient_id,
--         message_recipient_email,
--         message_date_time::Date AS message_open_date,
--         TRUE AS is_opened
--     FROM {{ref('ao_open')}}
--     WHERE message_verb = 'o'
--     AND message_date_time::Date >= '2023-01-01'
--     AND message_id LIKE 's-%'
    
), final AS (

    SELECT
        email_sent.message_id,
        email_sent.message_recipient_id,
        email_sent.message_recipient_email,
        message_send_date,
        -- message_open_date,
        message_click_date,
        is_sent,
        -- is_opened,
        is_clicked
    FROM email_sent
    -- LEFT JOIN email_opened
    --     ON email_sent.message_id=email_opened.message_id
    LEFT JOIN email_clicked
        ON email_sent.message_id=email_clicked.message_id
        AND LOWER(email_sent.message_recipient_email)=LOWER(email_clicked.message_recipient_email)
    WHERE message_send_date >= '2022-06-01'

)

SELECT *
FROM final
-- WHERE LOWER(message_recipient_email) = 'devon.roberts@techoregon.org'
-- -- AND is_clicked = True
-- AND message_id = 's-ec11-2212'
