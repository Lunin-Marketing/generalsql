{{ config(materialized='table') }}

SELECT
    "meetingId",
    "meetingRecId",
    "meetingEmailKey",
    "meetingName",
    "meetingRegistrationTime",
    "meetingStartTime",
    "accountId",
    "dateTime",
    "insertTime",
    "fingerprint"
FROM "9883Data".FACTS.BOGUSMEETING_9883