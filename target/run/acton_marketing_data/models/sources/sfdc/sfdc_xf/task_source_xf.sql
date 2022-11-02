

  create  table "acton"."dbt_actonmarketing"."task_source_xf__dbt_tmp"
  as (
    

WITH base AS (

SELECT *
FROM "acton"."salesforce"."task"

), renamed AS (

    SELECT
        id AS task_id,
        what_id,
        subject AS task_subject,
        activity_date,
        status AS task_status,
        owner_id,
        is_deleted,
        account_id,
        is_closed,
        created_date AS task_created_date,
        last_modified_date,
        system_modstamp,
        is_archived,
        record_type_id,
        who_id AS person_id,
        type AS task_type,
        closed_date_c AS close_date
    FROM base
    WHERE who_id IS NOT null
    AND base.is_deleted = 'False'

), final AS (

    SELECT
    --IDs
        task_id,
        renamed.person_id,
        owner_id,
        renamed.account_id,
        email,

    --Task Data
        task_subject,
        task_status,
        task_type,
        is_closed,
        activity_date,
        task_created_date,
        close_date
    FROM renamed
    LEFT JOIN "acton"."dbt_actonmarketing"."person_source_xf" ON
    renamed.person_id=person_source_xf.person_id
)

SELECT*
FROM final
  );