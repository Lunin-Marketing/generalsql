{{ config(materialized='table') }}

WITH email_sent AS (

    SELECT
        message_id,
        message_recipient_id,
        message_recipient_email,
        message_date_time::Date AS message_send_date,
        TRUE AS is_sent
    FROM {{ref('ao_sent')}}
    WHERE ao_sent.message_verb = 's'
    AND message_date_time::Date >= '2023-01-01'

), email_clicked AS (

    SELECT
        message_id,
        message_recipient_id,
        message_recipient_email,
        message_date_time::Date AS message_click_date,
        TRUE AS is_clicked
    FROM {{ref('ao_response')}}
    WHERE message_verb = 'c'
    AND message_date_time::Date >= '2023-01-01'

), email_opened AS (

    SELECT
        message_id,
        message_recipient_id,
        message_recipient_email,
        message_date_time::Date AS message_open_date,
        TRUE AS is_opened
    FROM {{ref('ao_open')}}
    WHERE message_verb = 'o'
    AND message_date_time::Date >= '2023-01-01'
    
), final AS (

    SELECT DISTINCT
        email_sent.message_id,
        email_sent.message_recipient_id,
        email_sent.message_recipient_email,
        message_send_date,
        message_open_date,
        message_click_date,
        is_sent,
        is_opened,
        is_clicked
    FROM email_sent
    LEFT JOIN email_opened
        ON email_sent.message_id=email_opened.message_id
    LEFT JOIN email_clicked
        ON email_sent.message_id=email_clicked.message_id
    WHERE message_send_date >= '2023-01-01'

)

SELECT *
FROM final
