SELECT
    user_full_name,
    delivered_date,
    COUNT(DISTINCT mailing_id) AS emails_sent
FROM {{ref('outreach_users_xf')}}
LEFT JOIN {{ref('outreach_sequence_xf')}}
    ON outreach_users_xf.user_id=outreach_sequence_xf.or_sequence_owner_id
LEFT JOIN {{ref('outreach_mailing_xf')}}
    ON outreach_sequence_xf.or_sequence_id=outreach_mailing_xf.relationship_sequence_id
WHERE delivered_date IS NOT null
GROUP BY 1,2
