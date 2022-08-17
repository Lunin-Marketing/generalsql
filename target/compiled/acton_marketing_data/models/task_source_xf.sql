
WITH base AS (
SELECT *
FROM "acton".salesforce.task

), final AS (

    SELECT
        id AS task_id,
        what_id,
        subject,
        activity_date,
        status,
        owner_id,
        is_deleted,
        account_id,
        is_closed,
        created_date AS task_created_date,
        last_modified_date,
        system_modstamp,
        is_archived,
        record_type_id,
        who_id,
        type,
        closed_date_c AS close_date
    FROM base

)

SELECT*
FROM final