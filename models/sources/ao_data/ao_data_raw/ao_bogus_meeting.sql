{{ config(materialized='table') }}

SELECT
    "meetingId" AS webinar_id,
    "meetingRecId" AS webinar_recipient_id,
    "meetingEmailKey" AS webinar_email,
    "meetingName" AS webinar_name,
    "meetingRegistrationTime" AS webinar_registration_date_time,
    "meetingStartTime" AS webinar_start_date_time,
    "accountId" AS webinar_account_id,
    "dateTime" AS webinar_date_time,
    "verbKey" AS webinar_verb,
    "insertTime" AS webinar_insert_date_time,
    "fingerprint" AS webinar_fingerprint
FROM "9883_DATA".FACTS.BOGUSMEETING_9883