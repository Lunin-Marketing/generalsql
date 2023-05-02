SELECT
    user_full_name,
    outreach_task_due_date,
    COUNT(DISTINCT mailing_id) AS emails_sent
FROM {{ref('outreach_users_xf')}}
LEFT JOIN {{ref('outreach_tasks_xf')}}
    ON outreach_users_xf.user_id=outreach_tasks_xf.relationship_owner_id
WHERE outreach_task_due_date IS NOT null
GROUP BY 1,2
